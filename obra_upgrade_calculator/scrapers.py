import logging
from datetime import datetime, timedelta

import requests
from lxml import html
from peewee import JOIN, fn

from .models import Event, Person, ObraPerson, Race, Result, Series

session = requests.Session()
logger = logging.getLogger(__name__)


def scrape_year(year, event_type):
    """
    Scrape all results for a given year
    Avoid scraping 'all' since it will break scoring
    """
    logger.info('Getting {} events for {}'.format(event_type, year))
    url = 'http://obra.org/results/{}/{}'.format(year, event_type)
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

        if element.get('class') == 'multi-day-event-child':
            # multi-day-event-child class used for series events
            logger.info('Found Event id={} name={} date={} with parent {}'.format(event_id, event_name, event_date, parent_id))
            (Event.insert(id=event_id,
                          name=event_name,
                          type=event_type,
                          year=year,
                          date=event_date,
                          series_id=parent_id)
                  .on_conflict_replace()
                  .execute())
        else:
            if '-' in event_date:
                # Assume anything with a date range is a series
                logger.info('Found Series id={} name={} dates={}'.format(event_id, event_name, event_date))
                (Series.insert(id=event_id,
                               name=event_name,
                               type=event_type,
                               year=year,
                               dates=event_date)
                       .on_conflict_replace()
                       .execute())
                parent_id = event_id
                parent_name = event_name
            else:
                # Single date with no parent, must be a standalone event
                logger.info('Found Event id={} name={} date={}'.format(event_id, event_name, event_date))
                (Event.insert(id=event_id,
                              name=event_name,
                              type=event_type,
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
                  .having(fn.Count(Race.id) == 0))
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


def scrape_event(event):
    """Scrape Race Results for a single Event"""
    logger.info("Scraping data for Event: [{}]{} on {}/{}".format(event.id, event.name, event.year, event.date))
    url = 'http://obra.org/events/{}/results.json'.format(event.id)
    response = session.get(url)
    response.raise_for_status()

    people = dict()
    races = dict()
    for result in response.json():
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
            # TODO: Check for Races going away entirely. Not sure this ever happens?
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
                    logger.info('Deleting old Results from this race')
                    prev_race.delete_instance(recursive=True)

            (Race.insert(id=result['race_id'],
                         event_id=result['event_id'],
                         name=result['race_name'],
                         date=result['date'],
                         created=datetime.strptime(result['created_at'][:19], '%Y-%m-%dT%H:%M:%S'),
                         updated=datetime.strptime(result['updated_at'][:19], '%Y-%m-%dT%H:%M:%S'))
                 .execute())

        # Load Persons and Results
        if races[result['race_id']]:
            if result['person_id'] and result['person_id'] not in people:
                people[result['person_id']] = True
                (Person.insert(id=result['person_id'],
                               first_name=result['first_name'],
                               last_name=result['last_name'])
                       .on_conflict_replace()
                       .execute())

            (Result.insert(id=result['id'],
                           race_id=result['race_id'],
                           person_id=result['person_id'],
                           place=result['place'])
                   .execute())


def scrape_person(person):
    logger.info('Scraping Person data for {}'.format(person.id))
    url = 'http://obra.org/people/{}/1900'.format(person.id)
    response = session.get(url)
    response.raise_for_status()

    kwargs = {'person': person, 'updated': datetime.now()}
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

    (ObraPerson.insert(**kwargs)
               .on_conflict_replace()
               .execute())
