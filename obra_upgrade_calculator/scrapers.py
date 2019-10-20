#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from datetime import date, datetime, timedelta

import requests
from lxml import html
from peewee import EXCLUDED, JOIN, fn

from .data import (AGE_RANGE_RE, CATEGORY_RE, DISCIPLINE_MAP,
                   DISCIPLINE_RE_MAP, STANDINGS_RE)
from .models import Event, ObraPersonSnapshot, Person, Race, Result, Series, db

session = requests.Session()
logger = logging.getLogger(__name__)
baseurl = 'https://obra.org'


@db.savepoint()
def scrape_year(year, upgrade_discipline):
    """
    Scrape all results for a given year and category
    """
    for discipline in DISCIPLINE_MAP[upgrade_discipline]:
        logger.info('Getting {} events for {}'.format(discipline, year))
        response = session.get('{}/results/{}/{}'.format(baseurl, year, discipline))
        response.raise_for_status()
        tree = html.fromstring(response.text)
        parent_id = ''
        parent_name = ''

        for element in tree.xpath('//table[contains(@class,"results_home")]//tr'):
            # No results early in the year
            if not element.xpath('td/a'):
                continue

            # Extract event names, dates, and IDs from link text
            event_anchor = element.xpath('td/a')[0]
            event_date = element.xpath('td[@class="date"]')[0].text
            event_id = event_anchor.get('href').split('/')[2]

            # Series results are linked by date; single events by name
            if event_date:
                event_date = event_date.strip()
                event_name = event_anchor.text
            else:
                event_date = event_anchor.text.strip()
                event_name = parent_name

            event_discipline = get_discipline(event_name, discipline)
            if not event_discipline:
                logger.warn('Found Event id={} name={} date={} with blacklisted discipline'.format(event_id, event_name, event_date))
                continue

            if element.get('class') == 'multi-day-event-child':
                # multi-day-event-child class used for series events
                logger.info('Found Event id={} name={} date={} discipline={} with series {}'.format(
                            event_id, event_name, event_date, event_discipline, parent_id))
                if parent_id:
                    (Event.insert(id=event_id,
                                  name=event_name,
                                  discipline=event_discipline,
                                  year=year,
                                  date=event_date,
                                  series_id=parent_id,
                                  parent_id=None)
                          .on_conflict(conflict_target=[Event.id],
                                       preserve=[Event.name, Event.discipline, Event.year, Event.date, Event.series, Event.parent])
                          .execute())
                else:
                    logger.warn('Found multi-day-event-child without a series!')
            else:
                if element.get('class') == 'multi-day-event' or '-' in event_date:
                    # Assume anything with a date range is a series
                    logger.info('Found Series id={} name={} dates={}'.format(event_id, event_name, event_date))
                    (Series.insert(id=event_id,
                                   name=event_name,
                                   year=year,
                                   dates=event_date)
                           .on_conflict(conflict_target=[Series.id],
                                        preserve=[Series.name, Series.year, Series.dates])
                           .execute())
                    parent_id = event_id
                    parent_name = event_name
                else:
                    # Single date with no parent, must be a standalone event
                    logger.info('Found Event id={} name={} date={} discipline={}'.format(event_id, event_name, event_date, event_discipline))
                    (Event.insert(id=event_id,
                                  name=event_name,
                                  discipline=event_discipline,
                                  year=year,
                                  date=event_date,
                                  series_id=None,
                                  parent_id=None)
                          .on_conflict(conflict_target=[Event.id],
                                       preserve=[Event.name, Event.discipline, Event.year, Event.date, Event.series, Event.parent])
                          .execute())


def scrape_parents(year, upgrade_discipline):
    """
    Scrape all events once to check to see if they've got any children.
    Unscraped races are not ignored, not a child of another event, not parent to another event, and don't have any races.
    This assumes that additional child events don't show up later.
    """
    logger.info('Scraping {} Events to check for children'.format(upgrade_discipline))
    event_count = 0
    query = (Event.select()
                  .join(Race, src=Event, join_type=JOIN.LEFT_OUTER)
                  .where(Event.year == year)
                  .where(Event.ignore == False)
                  .where(Event.parent_id.is_null(True))
                  .where(Event.id.not_in(Event.select(Event.parent_id)
                                              .where(Event.year == year)
                                              .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])))
                  .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                  .group_by(Event.id)
                  .having(fn.COUNT(Race.id) == 0))

    for event in query.execute():
        event_count += scrape_parent_event(event)

    return event_count


def scrape_new(upgrade_discipline):
    """Scrape all Events that do not yet have any Races loaded"""
    logger.info('Scraping all {} Events with no Races'.format(upgrade_discipline))
    race_count = 0
    query = (Event.select()
                  .join(Race, src=Event, join_type=JOIN.LEFT_OUTER)
                  .where(Event.ignore == False)
                  .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                  .group_by(Event.id)
                  .having(fn.COUNT(Race.id) == 0))

    for event in query:
        logger.info('Found Event [{}]{} with 0 races'.format(event.id, event.name))
        race_count += scrape_event(event)

    return race_count


def scrape_recent(upgrade_discipline, days):
    """
    Scrape all events that have had results created in the last N days
    Results frequently change for up to a week afterwards, so it's important to check back.
    """
    logger.info('Scraping {} Events Races updated in the last {} days'.format(upgrade_discipline, days))
    race_count = 0
    update_threshold = datetime.now() - timedelta(days=days)
    query = (Event.select(Event,
                          fn.MAX(Race.updated).alias('updated'))
                  .join(Race, src=Event)
                  .where(Event.ignore == False)
                  .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                  .group_by(Event.id)
                  .having(Race.updated > update_threshold))

    for event in query.execute():
        logger.info('Found recent Event {} - Results updated {}'.format(event.id, event.updated))
        race_count += scrape_event(event)

    return race_count


@db.savepoint()
def scrape_parent_event(event):
    """Scrape an event with children events. Not sure how this is different from a series?"""
    logger.info("Scraping data for potential parent Event: [{}]{} on {}/{}".format(event.id, event.name, event.year, event.date))
    change_count = 0
    response = session.get('{}/events/{}/results'.format(baseurl, event.id))
    response.raise_for_status()
    tree = html.fromstring(response.text)

    for event_anchor in tree.xpath('//div[contains(@class,"child_events")]//a'):
        event_id = event_anchor.get('href').split('/')[2]
        event_name = event_anchor.text

        event_discipline = get_discipline(event_name, event.discipline)
        if not event_discipline:
            logger.warn('Found Event id={} name={} date={} with blacklisted discipline'.format(event_id, event_name, event.date))
            continue

        if event.name not in event_name:
            event_name = '{}: {}'.format(event.name, event_name)

        logger.info('Found child Event id={} name={}'.format(event_id, event_name))
        change_count += (Event.insert(id=event_id,
                                      name=event_name,
                                      discipline=event_discipline,
                                      year=event.year,
                                      date=event.date,
                                      series_id=event.series_id,
                                      parent_id=event.id)
                              .on_conflict(conflict_target=[Event.id],
                                           preserve=[Event.name, Event.discipline, Event.year, Event.date, Event.series, Event.parent])
                              .execute())

    return change_count


@db.savepoint()
def scrape_event(event):
    """Scrape Race Results for a single Event"""
    logger.info('Scraping data for Event: [{}]{} on {}/{}'.format(event.id, event.name, event.year, event.date))

    response = session.get('{}/events/{}/results.json'.format(baseurl, event.id))
    response.raise_for_status()
    results = response.json()

    if not results:
        logger.warning('Skipping and ignoring Event: has no results!')
        event.ignore = True
        event.save()
        Result.delete().where(Result.race_id << (Race.select(Race.id).where(Race.event_id == event.id))).execute()
        return Race.delete().where(Race.event_id == event.id).execute()

    change_count = 0
    people = dict()
    races = dict()

    for result in results:
        # Do some preflight checks the first time we see a row with a new race_id
        if result['race_id'] not in races:
            races[result['race_id']] = True  # Load results by default

            logger.info('Processing Race: [{}]{}: [{}]{}'.format(
                result['event_id'], result['event_full_name'],
                result['race_id'], result['race_name']))

            # When results are updated, the race_id changes when the offical
            # uploads the new score sheet. Check for an old Race with a different
            # race_id but the same race_name. Theoretically the old results are still
            # in the OBRA DB somewhere?
            try:
                prev_race = (Race.select()
                                 .where(Race.event_id == event.id)
                                 .where(Race.name == result['race_name'])
                                 .get())
            except Race.DoesNotExist:
                prev_race = None

            # If we found an old race with results loaded, wipe 'em out and load new Results
            if prev_race:
                if prev_race.id == result['race_id']:
                    result_count = prev_race.results.count()
                    if result_count > 0:
                        logger.info('Already loaded {} Results for this Race'.format(result_count))
                        races[result['race_id']] = False  # Flag to disable result loading
                        continue
                else:
                    logger.info('Deleting old race [{}]{}'.format(prev_race.id, prev_race.name))
                    prev_race.delete_instance(recursive=True)

            (Race.insert(id=result['race_id'],
                         event_id=result['event_id'],
                         name=result['race_name'],
                         date=result['date'],
                         categories=get_categories(result['race_name'], event.discipline),
                         created=datetime.strptime(result['created_at'][:19], '%Y-%m-%dT%H:%M:%S'),
                         updated=datetime.strptime(result['updated_at'][:19], '%Y-%m-%dT%H:%M:%S'))
                 .execute())

        # Skip loading Results if flag is false for this Race
        if not races[result['race_id']]:
            continue

        # Create Person if necessary
        if result['person_id'] and result['person_id'] not in people:
            if result['first_name'] and result['last_name']:
                (Person.insert(id=result['person_id'],
                               first_name=result['first_name'],
                               last_name=result['last_name'],
                               team_name=result['team_name'] or '')
                       .on_conflict(conflict_target=[Person.id],
                                    preserve=[Person.team_name, Person.first_name, Person.last_name],
                                    where=(EXCLUDED.team_name != ''))
                       .execute())
            else:
                person = find_person(str(result['name']))
                if person:
                    result['person_id'] = person.id
                else:
                    logger.warning('Cannot find Person for corrupt Result with name {}'.format(result['name']))
                    continue
            people[result['person_id']] = True

        # Create Result
        (Result.insert(id=result['id'],
                       race_id=result['race_id'],
                       person_id=result['person_id'],
                       place=result['place'],
                       time=result['time'],
                       laps=result['laps'])
               .execute())

    # Calculate starting field size for scraped races
    # Count all the DNFs and DQs, but ignore DNS
    # Not sure how Candi did it but this makes sense to me
    for race_id, scrape_flag in races.items():
        if scrape_flag:
            starters = (Result.select()
                              .where(~(Result.place.contains('dns')))
                              .where(Result.race_id == race_id)
                              .count())
            logger.info('Counted {} starters for race [{}]'.format(starters, race_id))
            change_count += 1
            (Race.update({Race.starters: starters})
                 .where(Race.id == race_id)
                 .execute())

    # Delete any races not present in the scraped results
    for prev_race in event.races.select(Race.id, Race.name).where(Race.id.not_in([r for r in races])):
        logger.info('Deleting orphan race [{}]{}'.format(prev_race.id, prev_race.name))
        change_count += 1
        prev_race.delete_instance(recursive=True)

    logger.info('Event scrape modified {} Races'.format(change_count))
    return change_count


@db.savepoint()
def clean_events(year, upgrade_discipline):
    race_count = 0
    query = (Event.select()
                  .where(Event.year == year)
                  .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline]))

    for event in query:
        if STANDINGS_RE.search(event.name):
            logger.info('Ignoring Event: [{}]{} on {}/{}'.format(event.id, event.name, event.year, event.date))
            event.ignore = True
            event.save()
            Result.delete().where(Result.race_id << (Race.select(Race.id).where(Race.event_id == event.id))).execute()
            race_count += Race.delete().where(Race.event_id == event.id).execute()

    return race_count


def find_person(name):
    """
    Sometimes results come through with the name mangled and a new id.
    See if we can find an existing person with some combination of their first and last names.
    """
    if ' ' in name:
        name = name.replace(',', '')
    else:
        return None

    try:
        (first, last) = name.split(' ', 1)
        return Person.get(Person.first_name ** first, Person.last_name ** last)
    except Person.DoesNotExist:
        pass

    try:
        (last, first) = name.split(' ', 1)
        return Person.get(Person.first_name ** first, Person.last_name ** last)
    except Person.DoesNotExist:
        pass

    return None


@db.savepoint()
def scrape_person(person):
    logger.info('Scraping Person data for {}'.format(person.id))
    response = session.get('{}/people/{}/1900'.format(baseurl, person.id))
    response.raise_for_status()

    kwargs = {'person': person, 'date': date.today()}
    tree = html.fromstring(response.text)
    for attr in ['license', 'mtb_category', 'dh_category', 'ccx_category', 'road_category', 'track_category']:
        path = '//p[@id="person_{}"]'.format(attr)
        elem = tree.xpath(path)
        if elem and elem[0].text:
            try:
                value = int(elem[0].text)
            except ValueError:
                value = 0
            kwargs[attr] = value

    (ObraPersonSnapshot.insert(**kwargs)
                       .execute())


def get_categories(race_name, event_discipline):
    """
    Extract a category list from the race name
    """
    # FIXME - need to handle pro/elite (cat 0) for MTB
    # FIXME - MTB categories are a disaster and probably need a completely different set of patterns
    cat_match = CATEGORY_RE.search(race_name)
    age_match = AGE_RANGE_RE.search(race_name)
    if age_match:
        return []
    elif cat_match:
        cats = cat_match.group(1).lower()
        if cats in ['beginner', 'novice']:
            cats = '5'
        elif cats == 'c':
            cats = '4'
        elif cats == 'b':
            cats = '3'
        elif cats == 'a':
            cats = '1/2'
        elif cats == 'a/b':
            cats = '1/2/3'
        elif cats == 'b/c':
            cats = '3/4'
        return [int(c) for c in cats.split('/')]
    else:
        return []


def get_discipline(event_name, event_discipline):
    for upgrade_discipline, event_disciplines in DISCIPLINE_MAP.items():
        if event_discipline in event_disciplines:
            logger.debug('Using upgrade_discipline={} for event_discipline={}'.format(upgrade_discipline, event_discipline))
            if upgrade_discipline in DISCIPLINE_RE_MAP:
                for discipline_name, discipline_re in DISCIPLINE_RE_MAP[upgrade_discipline]:
                    logger.debug('Checking discipline_name={} discipline_re={} for event_name={}'.format(discipline_name, discipline_re.pattern, event_name))
                    if discipline_re.search(event_name):
                        logger.debug('Matched override discipline_name={}'.format(discipline_name))
                        return discipline_name
    logger.debug('Returning event_discipline={}'.format(event_discipline))
    return event_discipline
