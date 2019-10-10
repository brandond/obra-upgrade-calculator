#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from datetime import date, datetime, timedelta

import requests
from lxml import html
from peewee import JOIN, fn

from .data import AGE_RANGE_RE, CATEGORY_RE, DISCIPLINE_MAP, DISCIPLINE_RE_MAP
from .models import Event, ObraPersonSnapshot, Person, Race, Result, Series, db

session = requests.Session()
logger = logging.getLogger(__name__)


@db.atomic()
def scrape_year(year, discipline):
    """
    Scrape all results for a given year
    Avoid scraping 'all' since it will break scoring
    """
    logger.info('Getting {} events for {}'.format(discipline, year))
    url = 'http://obra.org/results/{}/{}'.format(year, discipline)
    response = session.get(url)
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
            logger.info('Found Event id={} name={} date={} discipline={} with series {}'.format(event_id, event_name, event_date, event_discipline, parent_id))
            if parent_id:
                (Event.insert(id=event_id,
                              name=event_name,
                              discipline=event_discipline,
                              year=year,
                              date=event_date,
                              series_id=parent_id)
                      .on_conflict_replace()
                      .execute())
            else:
                logger.warn('Found multi-day-event-child without a series!')
        else:
            if '-' in event_date:
                # Assume anything with a date range is a series
                logger.info('Found Series id={} name={} dates={}'.format(event_id, event_name, event_date))
                (Series.insert(id=event_id,
                               name=event_name,
                               year=year,
                               dates=event_date)
                       .on_conflict_replace()
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
                              date=event_date)
                      .on_conflict_replace()
                      .execute())


def scrape_new():
    """Scrape all Events that do not yet have any Races loaded"""
    logger.info('Scraping all Events with no Races')
    query = (Event.select()
                  .join(Race, JOIN.LEFT_OUTER)
                  .group_by(Event.id)
                  .having(fn.COUNT(Race.id) == 0))
    for event in query.execute():
        logger.info('Found unscraped Event {}'.format(event.id))
        scrape_event(event)


def scrape_recent(days):
    """
    Scrape all events that have had results created in the last N days
    Results frequently change for up to a week afterwards, so it's important to check back.
    """
    logger.info('Scraping Events with Results created in the last {} days'.format(days))
    create_threshold = datetime.now() - timedelta(days=days)
    query = (Event.select(Event,
                          fn.MAX(Race.created).alias('created'))
                  .join(Race)
                  .switch(Event)
                  .group_by(Event.id)
                  .having(Race.created > create_threshold))
    for event in query.execute():
        logger.info('Found recent Event {} - Results created {}'.format(event.id, event.created))
        scrape_event(event)


@db.atomic()
def scrape_event(event):
    """Scrape Race Results for a single Event"""
    logger.info("Scraping data for Event: [{}]{} on {}/{}".format(event.id, event.name, event.year, event.date))
    url = 'http://obra.org/events/{}/results.json'.format(event.id)
    response = session.get(url)
    response.raise_for_status()

    people = dict()
    races = dict()
    results = response.json()
    for result in results:
        # Do some preflight checks the first time we see a row with a new race_id
        if result['race_id'] not in races:
            races[result['race_id']] = True  # Load results by default

            logger.info('Processing Race: [{}]{}: [{}]{}'.format(
                result['event_id'], result['event_full_name'],
                result['race_id'], result['race_name']))

            if 'standings' in result['event_full_name'].lower():
                logging.warning('Skipping Race: Event appears to be series points total')
                races[result['race_id']] = False
                continue

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
                         categories=get_categories(result['race_name']),
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
                       .on_conflict_replace()
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
            starters = (Result.select(fn.COUNT(Result.id))
                              .where(~(Result.place.contains('dns')))
                              .where(Result.race_id == race_id)
                              .scalar())
            logger.info('Counted {} starters for race [{}]'.format(starters, race_id))
            (Race.update({Race.starters: starters})
                 .where(Race.id == race_id)
                 .execute())

    # Delete any races not present in the scraped results
    for prev_race in event.races.select(Race.id, Race.name).where(Race.id.not_in([r for r in races])):
        logger.info('Deleting orphan race [{}]{}'.format(prev_race.id, prev_race.name))
        prev_race.delete_instance(recursive=True)


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


def scrape_person(person):
    logger.info('Scraping Person data for {}'.format(person.id))
    url = 'http://obra.org/people/{}/1900'.format(person.id)
    response = session.get(url)
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
                       .on_conflict_replace()
                       .execute())


def get_categories(race_name):
    """
    Extract a category list from the race name
    """
    # FIXME - need to handle pro/elite (cat 0) for MTB
    cat_match = CATEGORY_RE.search(race_name)
    age_match = AGE_RANGE_RE.search(race_name)
    if age_match:
        return []
    elif cat_match:
        cats = cat_match.group(1)
        if cats.lower() in ['beginner', 'novice']:
            cats = '5'
        elif cats.lower() == 'c':
            cats = '4'
        elif cats.lower() == 'b':
            cats = '3'
        elif cats.lower() == 'a':
            cats = '1/2'
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
