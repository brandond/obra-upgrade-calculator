#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
import logging
from collections import namedtuple
from datetime import date

from peewee import JOIN, fn, prefetch

from .models import Event, Person, Points, Race, Result
from .outputs import get_writer
from .scrapers import scrape_person
from .data import NAME_RE, NUMBER_RE, SCHEDULE, UPGRADES, DISCIPLINE_MAP

logger = logging.getLogger(__name__)
Point = namedtuple('Point', 'value,date')


def recalculate_points(upgrade_discipline):
    """
    Create Points for qualifying Results for all Races of this type.
    """

    # Remove any previously calculated points for this event type
    (Points.delete()
           .where(Points.result_id << (Result.select(Result.id)
                                             .join(Race)
                                             .join(Event)
                                             .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])))
           .execute())

    # Get all races in the last year with a minimum number of starters, filtering out non-eligible fields
    query = (Race.select(Race, Event, fn.COUNT(Result.id).alias('result_count'))
                 .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
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
        points = get_points_schedule(race.event.discipline, race)

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


def sum_points(upgrade_discipline):
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
                             (Result.select(fn.COUNT(Result.id))
                                    .where(Result.race_id == Race.id)
                                    .where(~(Result.place.contains('dns')))).alias('result_count'),
                             Event.id,
                             Event.name,
                             Event.discipline)
                     .join(Person)
                     .switch(Result)
                     .join(Race)
                     .join(Event)
                     .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                     .order_by(Person.last_name.collate('NOCASE').asc(),
                               Person.first_name.collate('NOCASE').asc(),
                               Race.date.asc()))

    person = None
    is_woman = False
    had_points = False
    last_change = date(1970, 1, 1)
    cat_points = []
    categories = {9}
    upgrade_notes = set()

    for result in prefetch(results, Points):
        # Print a sum and reset stats when the person changes
        if person != result.person:
            person = result.person
            is_woman = False
            had_points = False
            last_change = date(1970, 1, 1)
            cat_points = []
            categories = {9}
            upgrade_notes.clear()

        result_points_value = result.points[0].value if result.points else 0

        # Only process finishes (no dnf/dns/dq) with a known category
        if NUMBER_RE.match(result.place) and result.race.categories:
            upgrade_category = max(categories) - 1
            had_points = had_points or bool(cat_points)

            # Don't have any gender information in results, flag person as woman by race participation
            if 'women' in result.race.name.lower():
                is_woman = True

            # Here's the goofy category change logic
            if (upgrade_category in result.race.categories and
                can_upgrade(upgrade_discipline, points_sum(cat_points, result.race.date), upgrade_category, len(cat_points))):
                # Was eligible for an upgrade, and raced in a field that includes the upgrade category, and the race was over 2 weeks ago
                if (date.today() - result.race.date).days > 14:
                    upgrade_notes.add('UPGRADED TO {} WITH {} POINTS'.format(upgrade_category, points_sum(cat_points, result.race.date)))
                    cat_points = []
                    last_change = result.race.date
                    categories = {upgrade_category}
            elif (not categories.intersection(result.race.categories) and
                  min(categories) > min(result.race.categories)):
                # Race category does not overlap with rider category, and the race cateogory is more skilled
                if categories == {9}:
                    # First result for this rider, assign rider current race category - which may be multiple, such as 1/2 or 3/4
                    categories = set(result.race.categories)
                else:
                    # Complain if they don't have enough points or races for the upgrade
                    if can_upgrade(upgrade_discipline, points_sum(cat_points, result.race.date), max(result.race.categories), len(cat_points), True):
                        upgrade_note = ''
                    else:
                        upgrade_note = 'PREMATURELY '
                    upgrade_note += 'UPGRADED TO {} WITH {} POINTS'.format(max(result.race.categories), points_sum(cat_points, result.race.date))
                    upgrade_notes.add(upgrade_note)
                    cat_points = []
                    last_change = result.race.date
                    categories = {max(result.race.categories)}
            elif (not categories.intersection(result.race.categories) and
                  max(categories) < max(result.race.categories)):
                # Race category does not overlap with rider category, and the race category is less skilled
                if is_woman and 'women' not in result.race.name.lower():
                    # Women can race down-category in a men's race
                    pass
                elif (result.race.date - last_change).days >= 365 or not had_points:
                    # They've never had any points or it's been a while since they upgraded, probably nobody cares, give them a downgrade
                    upgrade_notes.add('DOWNGRADED TO {}'.format(min(result.race.categories)))
                    cat_points = []
                    last_change = result.race.date
                    categories = {min(result.race.categories)}
                elif result.points:
                    upgrade_notes.add('NO POINTS FOR RACING BELOW CATEGORY')
                    result.points[0].value = 0
            elif (len(categories.intersection(result.race.categories)) < len(categories) and
                  len(categories) > 1):
                # Refine category for rider who'd only been seen in multi-category races
                categories.intersection_update(result.race.categories)

            cat_points.append(Point(result_points_value, result.race.date))

            if result.points:
                if needs_upgrade(result.person, upgrade_discipline, points_sum(cat_points, result.race.date), categories):
                    upgrade_notes.add('NEEDS UPGRADE')
                    result.points[0].needs_upgrade = True

                result.points[0].sum_categories = list(categories)
                result.points[0].sum_value = points_sum(cat_points, result.race.date)
                result.points[0].save()

            if upgrade_notes:
                if had_points and not result.points:
                    result.points = [Points.create(result=result, starters=result.result_count, sum_categories=list(categories))]
                if result.points:
                    result.points[0].notes = '; '.join(reversed(sorted(upgrade_notes)))
                    result.points[0].save()
                upgrade_notes.clear()

        logger.info('{0}, {1}: {2} points for {3}/{4} at {5}: {6} on {7} ({8} as {9} in {10} {11})'.format(
            result.person.last_name,
            result.person.first_name,
            result_points_value,
            result.place,
            result.result_count,
            result.race.event.name,
            result.race.name,
            result.race.date,
            len(cat_points),
            '/'.join(str(c) for c in categories),
            '/'.join(str(c) for c in result.race.categories) or '-',
            result.race.event.discipline))


def print_points(upgrade_discipline, output_format):
    """
    Print out points tally for each Person
    """
    cur_year = date.today().year
    start_date = date(cur_year - 1, 1, 1)

    upgrades_needed = (Points.select(Points,
                                     Result.place,
                                     Event.discipline,
                                     Person.id,
                                     Person.first_name,
                                     Person.last_name,
                                     fn.MAX(Race.date).alias('last_date'))
                             .join(Result)
                             .join(Person)
                             .switch(Result)
                             .join(Race)
                             .join(Event)
                             .where(Race.date >= start_date)
                             .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
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
                            Event.name,
                            Event.discipline)
                    .join(Result)
                    .join(Person)
                    .switch(Result)
                    .join(Race)
                    .join(Event)
                    .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                    .where(fn.LENGTH(Person.last_name) > 1)
                    .order_by(Person.last_name.collate('NOCASE').asc(),
                              Person.first_name.collate('NOCASE').asc(),
                              Race.date.asc()))

    person = None
    with get_writer(output_format, upgrade_discipline) as writer:
        writer.start_upgrades()
        for point in upgrades_needed.execute():
            # Confirm that they haven't already been upgraded on the site
            obra = get_obra_data(point.result.person)
            discipline = point.result.race.event.discipline
            if obra.category_for_discipline(discipline) >= min(point.sum_categories):
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


def get_points_schedule(event_discipline, race):
    """
    Get the points shedule for the race's gender, starter count, and discipline
    See: http://www.obra.org/upgrade_rules.html
    """
    field = 'women' if re.search('women|junior', race.name, re.I) else 'open'

    if event_discipline in SCHEDULE:
        if field in SCHEDULE[event_discipline]:
            field_size_list = SCHEDULE[event_discipline][field]
        else:
            field_size_list = SCHEDULE[event_discipline]['open']

        for field_size in field_size_list:
            if race.result_count >= field_size['min'] and race.result_count <= field_size['max']:
                return field_size['points']
    else:
        logger.warn('No points schedule for event_discipline={}'.format(event_discipline))

    return []


def needs_upgrade(person, upgrade_discipline, points_sum, categories):
    """
    Determine if the rider needs an upgrade for this discipline
    """
    is_cat_1 = False
    if categories == {1, 2}:
        obra = get_obra_data(person)
        is_cat_1 = obra.category_for_discipline(upgrade_discipline) == 1

    category = max(categories) - 1
    if category == 0 or is_cat_1:
        return False

    if upgrade_discipline in UPGRADES and category in UPGRADES[upgrade_discipline]:
        max_points = UPGRADES[upgrade_discipline][category].get('max')
        logger.debug('Checking upgrade_discipline={} points_sum={} category={} max_points={}'.format(
            upgrade_discipline, points_sum, category, max_points))
        return points_sum >= max_points
    else:
        logger.warn('No upgrade schedule for upgrade_discipline={}'.format(upgrade_discipline))

    return False


def can_upgrade(upgrade_discipline, points_sum, category, num_races, check_min_races=False):
    """
    Determine if the rider can upgrade to a given category, based on their current points and race count
    """
    if upgrade_discipline in UPGRADES and category in UPGRADES[upgrade_discipline]:
        min_points = UPGRADES[upgrade_discipline][category].get('min')
        min_races = UPGRADES[upgrade_discipline][category].get('races')
        logger.debug('Checking upgrade_discipline={} points_sum={} category={} num_races={} min_points={} min_races={}'.format(
            upgrade_discipline, points_sum, category, num_races, min_points, min_races))
        if check_min_races and min_races and num_races >= min_races:
            return True
        elif points_sum >= min_points:
            return True
        else:
            return False
    else:
        raise Exception('No upgrade schedule for upgrade_discipline={}'.format(upgrade_discipline))


def get_obra_data(person):
    obra = person.obra.get() if person.obra.count() else None
    if not obra or obra.is_expired:
        scrape_person(person)
        obra = person.obra.get()
    return obra


def points_sum(points, race_date):
    """
    Calculate a sum of points earned within the last year (plus a one-week grace period)
    """
    return sum(int(p.value) for p in points if (race_date - p.date).days <= 372)
