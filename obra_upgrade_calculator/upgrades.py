#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
import logging
from collections import namedtuple

from peewee import JOIN, fn, prefetch

from .models import Event, Person, Points, Race, Result
from .outputs import get_writer
from .scrapers import scrape_person

logger = logging.getLogger(__name__)
Point = namedtuple('Point', 'value,date')
NAME_RE = re.compile("^[a-z.'-]+", flags=re.I)
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
    Create Points for qualifying Results for all Races of this type.
    """

    # Remove any previously calculated points for this event type
    (Points.delete()
           .where(Points.result_id << (Result.select(Result.id)
                                             .join(Race)
                                             .join(Event)
                                             .where(Event.type == event_type)))
           .execute())

    # Get all races in the last year with a minimum number of starters, filtering out non-eligible fields
    query = (Race.select(Race, Event, fn.COUNT(Result.id).alias('result_count'))
                 .where(Event.type == event_type)
                 .where(Race.categories.length() > 0)
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
        points = get_points_schedule(event_type, race)

        if race.categories and points:
            # If everything looks good, get the top N finishers for this race and assign points
            results = (race.results.select(Result.id,
                                           Result.place,
                                           Person.id,
                                           Person.first_name,
                                           Person.last_name,
                                           (Result.place.cast('integer') - 1).alias('zplace'))
                                   .join(Person)
                                   .where(Result.place.cast('integer') > 0)
                                   .where(Result.place.cast('integer') <= len(points))
                                   .order_by(Result.place.cast('integer').asc()))
            for result in results.execute():
                if not (NAME_RE.match(result.person.first_name) and NAME_RE.match(result.person.last_name)):
                    logger.debug('Invalid name: {} {}'.format(result.person.first_name, result.person.last_name))
                    continue
                logger.info('{}, {}: {} points for {} in {} at {}: {}'.format(
                    result.person.last_name,
                    result.person.first_name,
                    points[result.zplace],
                    result.place,
                    '/'.join(str(c) for c in race.categories),
                    race.event.name,
                    race.name))
                (Points.insert(result=result,
                               starters=race.result_count,
                               value=points[result.zplace])
                       .execute())
        else:
            logger.info('Invalid category or insufficient starters for this field')


def sum_points(event_type, strict_upgrades=False):
    """
    Calculate running points totals and detect upgrades
    Attempts to do some guessing at category and upgrades based on race participation
    and acrued points, but there's a potential to get it wrong. It'd be nice if the site
    tracked historical rider categories, but all you get is a point in time snapshot at
    the time the data is retrieved.
    """
    results = (Result.select(Result.id,
                             Result.place,
                             Person,
                             Race.id,
                             Race.name,
                             Race.date,
                             Race.categories,
                             Event.id,
                             Event.name)
                     .join(Person)
                     .switch(Result)
                     .join(Race)
                     .join(Event)
                     .where(Event.type == event_type)
                     .order_by(Person.last_name.collate('NOCASE').asc(),
                               Person.first_name.collate('NOCASE').asc(),
                               Race.date.asc()))

    person = None
    had_points = False
    cat_points = []
    categories = {9}
    needed_upgrade = False
    upgrade_notes = set()

    for result in prefetch(results, Points):
        # Print a sum and reset stats when the person changes
        if person != result.person:
            person = result.person
            had_points = False
            cat_points = []
            categories = {9}
            needed_upgrade = False
            upgrade_notes.clear()

        if result.place.lower() == 'dnf' or not result.race.categories:
            logger.info('{0}, {1}: {2} points for {3} at {4}: {5} ({6} in {7})'.format(
                result.person.last_name,
                result.person.first_name,
                '-',
                result.place,
                result.race.event.name,
                result.race.name,
                '/'.join(str(c) for c in categories),
                '-'))
            continue

        upgrade_category = max(categories) - 1

        # Here's the goofy category change logic
        if strict_upgrades and needed_upgrade and upgrade_category in result.race.categories:
            # Needed an upgrade, and is racing in the new category - grant it
            upgrade_note = 'UPGRADED TO {} WITH {} POINTS'.format(upgrade_category, points_sum(cat_points, result.race.date))
            upgrade_notes.add(upgrade_note)
            cat_points = []
            needed_upgrade = False
            categories = {upgrade_category}
        elif not categories.intersection(result.race.categories) and min(categories) > min(result.race.categories):
            # Race category does not overlap with rider category, and the race cateogory is more skilled
            if categories == {9}:
                # First result for this rider, assign rider current race category - which may be multiple, such as 1/2 or 3/4
                categories = set(result.race.categories)
            else:
                # Complain if they don't have enough points for the upgrade
                if can_upgrade(event_type, points_sum(cat_points, result.race.date), max(result.race.categories)):
                    upgrade_note = ''
                else:
                    upgrade_note = 'PREMATURELY '
                upgrade_note += 'UPGRADED TO {} WITH {} POINTS'.format(max(result.race.categories), points_sum(cat_points, result.race.date))
                upgrade_notes.add(upgrade_note)
                cat_points = []
                needed_upgrade = False
                categories = {max(result.race.categories)}
        elif not categories.intersection(result.race.categories) and max(categories) < max(result.race.categories):
            # Race category does not overlap with rider category, and the race category is less skilled
            if not had_points:
                # They've never had any points, probably nobody cares, give them a downgrade
                categories = {min(result.race.categories)}
                upgrade_notes.add('DOWNGRADED TO {}'.format(min(result.race.categories)))
                cat_points = []
            elif result.points:
                upgrade_notes.add('NO POINTS FOR RACING BELOW CATEGORY')
                result.points[0].value = 0
        elif len(categories.intersection(result.race.categories)) < len(categories) and len(categories) > 1:
            # Refine category for rider who'd only been seen in multi-category races
            categories.intersection_update(result.race.categories)

        had_points = had_points or bool(cat_points)

        if result.points:
            cat_points.append(Point(result.points[0].value, result.race.date))
            if needs_upgrade(result.person, event_type, points_sum(cat_points, result.race.date), categories):
                upgrade_notes.add('NEEDS UPGRADE')
                result.points[0].needs_upgrade = True
                needed_upgrade = True

            result.points[0].sum_categories = list(categories)
            result.points[0].sum_value = points_sum(cat_points, result.race.date)
            result.points[0].save()

        if upgrade_notes:
            if had_points and not result.points:
                result.points = [Points.create(result=result, sum_categories=list(categories))]
            if result.points:
                result.points[0].notes = '; '.join(reversed(sorted(upgrade_notes)))
                result.points[0].save()
            upgrade_notes.clear()

        logger.info('{0}, {1}: {2} points for {3} at {4}: {5} ({6} in {7})'.format(
            result.person.last_name,
            result.person.first_name,
            result.points[0].value if result.points else '-',
            result.place,
            result.race.event.name,
            result.race.name,
            '/'.join(str(c) for c in categories),
            '/'.join(str(c) for c in result.race.categories)))


def print_points(event_type, output_format):
    """
    Print out points tally for each Person
    """
    upgrades_needed = (Points.select(Points,
                                     Result.place,
                                     Person.id,
                                     Person.first_name,
                                     Person.last_name,
                                     fn.MAX(Race.date).alias('last_date'))
                             .join(Result)
                             .join(Person)
                             .switch(Result)
                             .join(Race)
                             .join(Event)
                             .where(Event.type == event_type)
                             .group_by(Person.id)
                             .having(Points.needs_upgrade == True)
                             .order_by(Points.sum_categories.asc(),
                                       Points.sum_value.desc(),
                                       Person.last_name.collate('NOCASE').asc(),
                                       Person.first_name.collate('NOCASE').asc()))

    points = (Points.select(Points,
                            Result,
                            Person,
                            Race.id,
                            Race.name,
                            Race.date,
                            Race.categories,
                            Event.id,
                            Event.name)
                    .join(Result)
                    .join(Person)
                    .switch(Result)
                    .join(Race)
                    .join(Event)
                    .where(Event.type == event_type)
                    .where(fn.LENGTH(Person.last_name) > 1)
                    .order_by(Person.last_name.collate('NOCASE').asc(),
                              Person.first_name.collate('NOCASE').asc(),
                              Race.date.asc()))

    person = None
    with get_writer(output_format, event_type) as writer:

        writer.start_upgrades()
        for point in upgrades_needed.execute():
            # Confirm that they haven't already been upgraded on the site
            obra = get_obra_data(point.result.person)
            if obra.category(event_type) >= min(point.sum_categories):
                writer.upgrade(point)
        writer.end_upgrades()

        for point in points.execute():
            if person != point.result.person:
                if person:
                    writer.end_person(person)
                person = point.result.person
                writer.start_person(person)
            writer.point(point)
        else:
            writer.end_person(person, True)


def get_points_schedule(event_type, race):
    """
    Get the points shedule for the race's gender and starter count
    See: http://www.obra.org/upgrade_rules.html
    """
    field = 'women' if re.search('women|junior', race.name, re.I) else 'open'

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
    if categories == {1, 2}:
        obra = get_obra_data(person)
        is_cat_1 = obra.category(event_type) == 1

    # FIXME - need to handle pro/elite (cat 0) for MTB
    if categories == {1} or is_cat_1:
        return False
    elif 2 in categories and 3 not in categories:
        return points_sum >= 35
    else:
        return points_sum >= 20


def can_upgrade(event_type, points_sum, category):
    """
    Determine if the rider is allowed to upgrade to a given category, based on their current points
    """
    if category in [1, 2]:
        return points_sum >= 20
    else:
        return True


def get_obra_data(person):
    obra = person.obra.get() if person.obra.count() else None
    if not obra or obra.is_expired():
        scrape_person(person)
        obra = person.obra.get()
    return obra


def points_sum(points, race_date):
    """
    Calculate a sum of points earned within the last year (plus a one-week grace period)
    """
    return sum(int(p.value) for p in points if (race_date - p.date).days <= 372)
