from __future__ import print_function

import logging
import re
from datetime import datetime, timedelta

from peewee import JOIN, fn

from .models import Event, Person, Points, Race, Result

logger = logging.getLogger(__name__)
CATEGORY_RE = re.compile(r'(beginner|\d/\d|\d)(?!\d?\+)', flags=re.I)
SCHEDULE = {
    'cyclocross': {
        'open': [
            {'min': 10, 'max': 15,  'points': [3,  2, 1]},
            {'min': 16, 'max': 25,  'points': [5,  4, 3, 2, 1]},
            {'min': 26, 'max': 60,  'points': [7,  6, 5, 4, 3, 2, 1]},
            {'min': 61, 'max': 999, 'points': [10, 8, 7, 5, 4, 3, 2, 1]},
        ],
        'women': [
            {'min':  6, 'max': 10,  'points': [3,  2, 1]},
            {'min': 11, 'max': 20,  'points': [5,  4, 3, 2, 1]},
            {'min': 21, 'max': 50,  'points': [7,  6, 5, 4, 3, 2, 1]},
            {'min': 51, 'max': 999, 'points': [10, 8, 7, 5, 4, 3, 2, 1]},
        ],
    }}


def recalculate_points(event_type):
    """
    Recalculate points totals for all races of this type.
    Note that this is hardcoded to give a point-in-time result from the date the script is run.
    """
    # Remove any previously calculated points for this event type
    (Points.delete()
           .where(Points.race_id << (Race.select(Race.id)
                                         .join(Event)
                                         .where(Event.type == event_type)))
           .execute())

    date_threshold = datetime.now() - timedelta(days=365)
    print('--- Upgrade Points Earned In {} Races Since {} ---\n'.format(
        event_type.capitalize(), date_threshold.strftime('%Y-%m-%d')))

    # Get all races in the last year with a minimum number of starters, filtering out non-eligible fields
    # TODO: Make race name filter more portable to non-CX events
    query = (Race.select(Race, Event, fn.COUNT(Result.id).alias('result_count'))
                 .where(Event.type == event_type)
                 .where(~(Race.name.contains('junior')))
                 .where(~(Race.name.contains('athena')))
                 .where(~(Race.name.contains('clyde')))
                 .where(~(Race.name.contains('stampede')))
                 .where(~(Race.name.contains('single')))
                 .where(~(Result.place.contains('dns')))
                 .join(Event)
                 .switch(Race)
                 .join(Result, JOIN.LEFT_OUTER)
                 .group_by(Race.id)
                 .having(Race.date >= date_threshold)
                 .having(fn.COUNT(Result.id) >= 6))

    for race in query.execute():
        logger.info('Got Race [{}]{}: [{}]{} on {} with {} starters'.format(
            race.event.id, race.event.name, race.id, race.name, race.date, race.result_count))

        # Extract categories from field name and check points depth for gender and field size
        categories = get_categories(race)
        points = get_points_schedule(event_type, race)

        if categories and points:
            # If everything looks good, get the top N finishers for this race and assign points
            results = (race.results.select(Result.place,
                                           Result.person_id,
                                           (Result.place.cast('integer') - 1).alias('zplace'))
                                   .where(Result.place.cast('integer') > 0)
                                   .where(Result.place.cast('integer') <= len(points))
                                   .order_by(Result.place.cast('integer').asc()))
            for result in results.execute():
                (Points.insert(person_id=result.person_id,
                               race_id=race.id,
                               categories=categories,
                               place=result.place,
                               starters=race.result_count,
                               points=points[result.zplace])
                       .execute())
        else:
            logger.info('Invalid category or insufficient starters for this field')


def get_categories(race):
    """
    Extract a category list from the race name
    """
    match = re.search(CATEGORY_RE, race.name)
    if match:
        cats = match.group(0)
        if cats.lower() == 'beginner':
            cats = '4/5'
        return [int(c) for c in cats.split('/')]
    else:
        return []


def get_points_schedule(event_type, race):
    """
    Get the points shedule for the race's gender and starter count
    See: http://www.obra.org/upgrade_rules.html
    """
    field = 'women' if 'women' in race.name.lower() else 'open'

    if event_type in SCHEDULE:
        for tier in SCHEDULE[event_type][field]:
            if race.result_count >= tier['min'] and race.result_count <= tier['max']:
                return tier['points']
    return []


def print_points(event_type):
    """
    Print out points tally for each Person
    Attempts to do some guessing at category and upgrades based on race participation
    and acrued points, but there's a potential to get it wrong. It'd be nice if the site
    tracked historical rider categories, but all you get is a point in time snapshot at
    the time the data is retrieved.
    """
    query = (Points.select(Points,
                           Person,
                           Race.name,
                           Race.date,
                           Event.name)
                   .join(Person)
                   .switch(Points)
                   .join(Race)
                   .join(Event)
                   .where(Event.type == event_type)
                   .where(fn.LENGTH(Person.last_name) > 1)
                   .order_by(Person.last_name.collate('NOCASE').asc(),
                             Person.first_name.collate('NOCASE').asc(),
                             Race.date.asc()))

    person = None
    person_note = ''
    points_sum = 0
    categories = {9}

    for point in query.execute():
        upgrade_note = ''
        # Print a sum and reset stats when the person changes
        if person and person != point.person:
            print_sum(person, categories, points_sum, person_note)
            person_note = ''
            points_sum = 0
            categories = {9}

        # Here's the goofy category change logic
        if not categories.intersection(point.categories) and min(categories) > min(point.categories):
            # Points were earned in a more skilled category than they were previously seen in - must have upgraded
            if categories != {9}:
                upgrade_note = '<-- UPGRADED AFTER {} POINTS'.format(points_sum)
                points_sum = 0
            categories = set(point.categories)
        elif not categories.intersection(point.categories) and max(categories) < max(point.categories):
            # Shamelessly racing completely below their pay grade; may God have mercy on their soul
            upgrade_note = '*** NO POINTS FOR RACING BELOW CATEGORY ***'
            person_note = '*** MAY NEED UPGRADE REMINDER ***'
            point.points = 0
        elif categories != set(point.categories) and len(categories.intersection(point.categories)) == 1:
            # Handle points earned in mixed fields or when moving between mixed and single fields
            if needs_upgrade(points_sum, categories):
                # Needed an upgrade, bump them into the more skilled category
                upgrade_note = '<-- UPGRADED AFTER {} POINTS'.format(points_sum)
                categories = {min(point.categories)}
                points_sum = 0
            elif categories.intersection(point.categories) != categories:
                # Previously seen in a mixed field, earned points in a single, now we know what they are
                categories = categories.intersection(point.categories)

        person = point.person
        points_sum += point.points
        print_single_point(point, categories, upgrade_note)
    else:
        print_sum(person, categories, points_sum, person_note)


def print_single_point(point, categories, upgrade_note):
    """Print a single Point"""
    print('      {0:<24s}: {1:>2d} points in Cat {2:<3s} - {3:d}/{4:<2d} at {5}: {6} on {7}  {8}'.format(
        ', '.join([point.person.last_name, point.person.first_name]),
        point.points,
        '/'.join(str(c) for c in categories),
        point.place,
        point.starters,
        point.race.event.name,
        point.race.name,
        point.race.date,
        upgrade_note))


def print_sum(person, categories, points_sum, person_note):
    """Print the sum of all points earned since the last upgrade"""
    name = ', '.join([person.last_name, person.first_name])
    upgrade_slug = '*** NEEDS UPGRADE ***' if needs_upgrade(points_sum, categories) and min(categories) > 1 else person_note
    print('-SUM- {0:<24s}: {1:>2d} points {2}\n'.format(name, points_sum, upgrade_slug))


def needs_upgrade(points_sum, categories):
    """
    Determine if the rider needs an upgrade
    Note that folks that are only ever seen in a 1/2 field are hard to handle,
    since we don't know if they're a 2 working on an upgrade to 1, or just a maxed-
    out 1.
    """
    if categories == {1}:
        return False
    elif 2 in categories:
        return points_sum >= 35
    else:
        return points_sum >= 20
