from __future__ import print_function

import logging
import re
from datetime import datetime, timedelta

from peewee import JOIN, fn

from .models import Event, Person, Points, Race, Result
from .outputs import get_writer
from .scrapers import scrape_person

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
    """
    # Remove any previously calculated points for this event type
    (Points.delete()
           .where(Points.race_id << (Race.select(Race.id)
                                         .join(Event)
                                         .where(Event.type == event_type)))
           .execute())

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
                                           Person.id,
                                           Person.first_name,
                                           Person.last_name,
                                           (Result.place.cast('integer') - 1).alias('zplace'))
                                   .join(Person)
                                   .where(Result.place.cast('integer') > 0)
                                   .where(Result.place.cast('integer') <= len(points))
                                   .order_by(Result.place.cast('integer').asc()))
            for result in results.execute():
                logger.info('{}, {}: {} points for {}'.format(
                    result.person.last_name,
                    result.person.first_name,
                    points[result.zplace],
                    result.place))
                (Points.insert(person_id=result.person.id,
                               race_id=race.id,
                               categories=categories,
                               place=result.place,
                               starters=race.result_count,
                               points=points[result.zplace])
                       .execute())
        else:
            logger.info('Invalid category or insufficient starters for this field')


def sum_points(event_type):
    start_date = datetime.now() - timedelta(days=395)

    query = (Points.select(Points,
                           Person,
                           Race.id,
                           Race.name,
                           Race.date,
                           Event.id,
                           Event.name)
                   .join(Person)
                   .switch(Points)
                   .join(Race)
                   .join(Event)
                   .where(Event.type == event_type)
                   .where(Race.date >= start_date)
                   .where(fn.LENGTH(Person.last_name) > 1)
                   .order_by(Person.last_name.collate('NOCASE').asc(),
                             Person.first_name.collate('NOCASE').asc(),
                             Race.date.asc()))

    person = None
    points_sum = 0
    categories = {9}
    needed_upgrade = False

    for point in query.execute():
        upgrade_notes = []
        # Print a sum and reset stats when the person changes
        if person != point.person:
            person = point.person
            points_sum = 0
            categories = {9}
            needed_upgrade = False

        # Here's the goofy category change logic
        if needed_upgrade and min(point.categories) == max(categories) - 1:
            upgrade_notes.append('UPGRADED TO {} AFTER {} POINTS'.format(max(categories) - 1, points_sum))
            points_sum = 0
            needed_upgrade = False
            categories = {max(categories) - 1}
        elif not categories.intersection(point.categories) and min(categories) > min(point.categories):
            if categories == {9}:
                categories = set(point.categories)
            elif can_upgrade(event_type, points_sum, max(point.categories)):
                    upgrade_notes.append('UPGRADED TO {} AFTER {} POINTS'.format(max(point.categories), points_sum))
                    points_sum = 0
                    needed_upgrade = False
                    categories = {max(point.categories)}
            else:
                upgrade_notes.append('NO POINTS FOR RACING ABOVE CATEGORY')
                point.points = 0
        elif not categories.intersection(point.categories) and max(categories) < max(point.categories):
            upgrade_notes.append('NO POINTS FOR RACING BELOW CATEGORY')
            point.points = 0
        elif len(categories.intersection(point.categories)) == 1 and len(categories) > 1:
            categories.intersection_update(point.categories)

        points_sum += point.points

        if needs_upgrade(point.person, event_type, points_sum, categories):
            upgrade_notes.append('NEEDS UPGRADE')
            point.needs_upgrade = True
            needed_upgrade = True

        point.sum_categories = list(categories)
        point.sum_points = points_sum
        point.sum_notes = '; '.join(upgrade_notes)
        point.save()


def print_points(event_type, output_format):
    """
    Print out points tally for each Person
    Note that this is hardcoded to give a point-in-time result from the date the script is run.
    Attempts to do some guessing at category and upgrades based on race participation
    and acrued points, but there's a potential to get it wrong. It'd be nice if the site
    tracked historical rider categories, but all you get is a point in time snapshot at
    the time the data is retrieved.
    """
    start_date = datetime.now() - timedelta(days=395)

    latest_points = (Points.select(Points,
                                   Person.id,
                                   Person.first_name,
                                   Person.last_name,
                                   fn.MAX(Race.date).alias('last_date'))
                           .join(Person)
                           .switch(Points)
                           .join(Race)
                           .group_by(Person.id)
                           .having(Points.needs_upgrade == True)
                           .order_by(Points.sum_categories.asc(),
                                     Points.sum_points.desc(),
                                     Person.last_name.collate('NOCASE').asc(),
                                     Person.first_name.collate('NOCASE').asc()))

    query = (Points.select(Points,
                           Person,
                           Race.id,
                           Race.name,
                           Race.date,
                           Event.id,
                           Event.name)
                   .join(Person)
                   .switch(Points)
                   .join(Race)
                   .join(Event)
                   .where(Event.type == event_type)
                   .where(Race.date >= start_date)
                   .where(fn.LENGTH(Person.last_name) > 1)
                   .order_by(Person.last_name.collate('NOCASE').asc(),
                             Person.first_name.collate('NOCASE').asc(),
                             Race.date.asc()))

    person = None
    with get_writer(output_format, event_type, start_date) as writer:

        writer.start_upgrades()
        for point in latest_points.execute():
            # Confirm that they haven't already been upgraded on the site
            obra = point.person.obra.get() if point.person.obra.count() else None
            if not obra or obra.is_expired():
                scrape_person(point.person)
                obra = point.person.obra.get()
            if obra.category(event_type) >= min(point.sum_categories):
                writer.upgrade(point)
        writer.end_upgrades()

        for point in query.execute():
            if person != point.person:
                if person:
                    writer.end_person(person)
                writer.start_person(point.person)
                person = point.person
            writer.point(point)
        else:
            writer.end_person(person, True)


def get_categories(race):
    """
    Extract a category list from the race name
    """
    # FIXME - need to handle pro/elite (cat 0) for MTB
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


def needs_upgrade(person, event_type, points_sum, categories):
    """
    Determine if the rider needs an upgrade
    """
    is_cat_1 = False
    if categories == {1,2}:
        obra = person.obra.get() if person.obra.count() else None
        if not obra or obra.is_expired():
            scrape_person(person)
            obra = person.obra.get()
        is_cat_1 = obra.category(event_type) == 1

    # FIXME - need to handle pro/elite (cat 0) for MTB
    if categories == {1} or is_cat_1:
        return False
    elif 2 in categories and not 3 in categories:
        return points_sum >= 35
    else:
        return points_sum >= 20


def can_upgrade(event_type, points_sum, category):
    if category == 1:
        return points_sum >= 35
    elif category == 2:
        return points_sum >= 20
    else:
        return True
