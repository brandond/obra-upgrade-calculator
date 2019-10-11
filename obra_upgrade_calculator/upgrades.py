#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import re
from collections import namedtuple
from datetime import date

from peewee import fn, prefetch

from .data import (DISCIPLINE_MAP, NAME_RE, NUMBER_RE, SCHEDULE_2018,
                   SCHEDULE_2019, SCHEDULE_2019_DATE, UPGRADES)
from .models import Event, ObraPersonSnapshot, Person, Points, Race, Result, db
from .outputs import get_writer
from .scrapers import scrape_person

logger = logging.getLogger(__name__)
Point = namedtuple('Point', 'value,place,date')


@db.atomic()
def recalculate_points(upgrade_discipline):
    """
    Create Points for qualifying Results for all Races of this type.
    """

    # Delete all Result data for this discipline and recalc from scratch
    # FIXME - add incremental support and make complete recalc selectable
    (Points.delete()
           .where(Points.result_id << (Result.select(Result.id)
                                             .join(Race, src=Result)
                                             .join(Event, src=Race)
                                             .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])))
           .execute())

    # Get all categorized races
    query = (Race.select(Race, Event)
                 .join(Event, src=Race)
                 .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                 .where(Race.categories.length() > 0))

    for race in query.execute():
        logger.info('Got Race [{}]{}: [{}]{} on {} with {} starters'.format(
            race.event.id, race.event.name, race.id, race.name, race.date, race.starters))

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
                                   .join(Person, src=Result)
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
                               value=points[result.zplace])
                       .execute())
        else:
            logger.info('Invalid category or insufficient starters for this field')


@db.atomic()
def sum_points(upgrade_discipline):
    """
    Calculate running points totals and detect upgrades
    Attempts to do some guessing at category and upgrades based on race participation
    and acrued points, but there's a potential to get it wrong. It'd be nice if the site
    tracked historical rider categories, but all you get is a point in time snapshot at
    the time the data is retrieved.
    """
    # Note that Race IDs don't necessarily imply the actual order that the races occurred
    # at the event. However, due to the way the site assigns created/updated
    # values, and the fact that the races are usually listed in order of occurrence in the
    # spreadsheet that is uploaded, we generally can imply actual order from the timestamps.
    results = (Result.select(Result.id,
                             Result.place,
                             Person,
                             Race.id,
                             Race.name,
                             Race.date,
                             Race.categories,
                             Race.starters,
                             Event.id,
                             Event.name,
                             Event.discipline)
                     .join(Person, src=Result)
                     .join(Race, src=Result)
                     .join(Event, src=Race)
                     .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                     .order_by(Person.last_name.collate('NOCASE').asc(),
                               Person.first_name.collate('NOCASE').asc(),
                               Race.date.asc(),
                               Race.created.asc()))

    person = None
    is_woman = False
    cat_points = []
    categories = {9}
    prev_race_categories = []
    upgrade_notes = set()
    upgrade_race = Race(date=date(1970, 1, 1))

    for result in prefetch(results, Points):
        # Print a sum and reset stats when the person changes
        if person != result.person:
            person = result.person
            is_woman = False
            cat_points[:] = []
            categories = {9}
            upgrade_notes.clear()
            prev_race_categories[:] = []
            upgrade_race = Race(date=date(1970, 1, 1))

        def result_points_value():
            return result.points[0].value if result.points else 0

        def points_sum():
            return sum(int(p.value) for p in cat_points)

        expired_points = expire_points(cat_points, result.race.date)
        if expired_points:
            upgrade_notes.add('{} {} EXPIRED'.format(expired_points, 'POINT HAS' if expired_points == 1 else 'POINTS HAVE'))

        # Only process finishes (no dns) with a known category
        if NUMBER_RE.match(result.place) and result.race.categories:
            upgrade_category = max(categories) - 1

            # Don't have any gender information in results, flag person as woman by race participation
            # I should call this is_not_cis_male or something lol
            if 'women' in result.race.name.lower():
                is_woman = True

            # Here's the goofy category change logic
            if   (upgrade_category in result.race.categories and
                  can_upgrade(upgrade_discipline, points_sum(), upgrade_category, cat_points) and
                  needs_upgrade(result.person, upgrade_discipline, points_sum(), categories, cat_points) and
                  prev_race_categories != result.race.categories):
                # Was eligible for and needed an upgrade, and raced in a different field that includes the upgrade category
                upgrade_notes.add('UPGRADED TO {} WITH {} POINTS'.format(upgrade_category, points_sum()))
                cat_points[:] = []
                categories = {upgrade_category}
                upgrade_race = result.race
            elif (not categories.intersection(result.race.categories) and
                  min(categories) > min(result.race.categories)):
                # Race category does not overlap with rider category, and the race cateogory is more skilled
                if categories == {9}:
                    # First result for this rider, assign rider current race category - which may be multiple, such as 1/2 or 3/4
                    if result.race.categories in ([1], [1, 2], [1, 2, 3]):
                        # If we first saw them racing as a pro they've probably been there for a while.
                        # Just check the site and assign their category from that.
                        obra_category = get_obra_data(result.person, result.race.date).category_for_discipline(result.race.event.discipline)
                        logger.info('OBRA category check: obra={}, race={}'.format(obra_category, result.race.categories))
                        if obra_category in result.race.categories:
                            categories = {obra_category}
                        else:
                            categories = {max(result.race.categories)}
                    else:
                        categories = set(result.race.categories)
                    # Add a dummy point and note to ensure Points creation
                    upgrade_notes.add('')
                else:
                    # Complain if they don't have enough points or races for the upgrade
                    if can_upgrade(upgrade_discipline, points_sum(), max(result.race.categories), cat_points, True):
                        upgrade_note = ''
                    else:
                        upgrade_note = 'PREMATURELY '
                    upgrade_note += 'UPGRADED TO {} WITH {} POINTS'.format(max(result.race.categories), points_sum())
                    cat_points[:] = []
                    upgrade_notes.add(upgrade_note)
                    categories = {max(result.race.categories)}
                    upgrade_race = result.race
            elif (not categories.intersection(result.race.categories) and
                  max(categories) < max(result.race.categories)):
                # Race category does not overlap with rider category, and the race category is less skilled
                if is_woman and 'women' not in result.race.name.lower():
                    # Women can race down-category in a men's race
                    pass
                elif not points_sum() and (result.race.date - upgrade_race.date).days > 365:
                    # All their points expired and it's been a year since they changed categories, probably nobody cares, give them a downgrade
                    cat_points[:] = []
                    upgrade_notes.add('DOWNGRADED TO {}'.format(min(result.race.categories)))
                    categories = {min(result.race.categories)}
                    upgrade_race = result.race
                elif result.points:
                    upgrade_notes.add('NO POINTS FOR RACING BELOW CATEGORY')
                    result.points[0].value = 0
            elif (len(categories.intersection(result.race.categories)) < len(categories) and
                  len(categories) > 1):
                # Refine category for rider who'd only been seen in multi-category races
                categories.intersection_update(result.race.categories)
                upgrade_notes.add('')
        elif result.points:
            logger.warn('Have points for a race with place={} and categories={}'.format(result.place, result.race.categories))

        cat_points.append(Point(result_points_value(), result.place, result.race.date))

        if (upgrade_race == result.race or upgrade_notes) and not result.points:
            result.points = [Points.create(result=result, value=0)]

        if result.points:
            if needs_upgrade(result.person, upgrade_discipline, points_sum(), categories, cat_points):
                upgrade_notes.add('NEEDS UPGRADE')
                result.points[0].needs_upgrade = True

            result.points[0].sum_categories = list(categories)
            result.points[0].sum_value = points_sum()
            result.points[0].save()

            if upgrade_notes:
                result.points[0].notes = '; '.join(reversed(sorted(n.capitalize() for n in upgrade_notes if n)))
                result.points[0].save()
                upgrade_notes.clear()

        prev_race_categories[:] = result.race.categories

        logger.info('{0}, {1}: {2} points for {3}/{4} at [{5}]{6}: {7} on {8} ({9} in {10} {11})'.format(
            result.person.last_name,
            result.person.first_name,
            result_points_value(),
            result.place,
            result.race.starters,
            result.race.id,
            result.race.event.name,
            result.race.name,
            result.race.date,
            '/'.join(str(c) for c in categories),
            '/'.join(str(c) for c in result.race.categories) or '-',
            result.race.event.discipline))


@db.atomic()
def print_points(upgrade_discipline, output_format):
    """
    Print out points tally for each Person
    """
    if output_format == 'null':
        return

    cur_year = date.today().year
    start_date = date(cur_year - 1, 1, 1)

    upgrades_needed = (Points.select(Points,
                                     Result.place,
                                     Event.discipline,
                                     Person.id,
                                     Person.first_name,
                                     Person.last_name,
                                     fn.MAX(Race.date).alias('last_date'))
                             .join(Result, src=Points)
                             .join(Person, src=Result)
                             .join(Race, src=Result)
                             .join(Event, src=Race)
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
                            Race.starters,
                            Race.categories,
                            Event.id,
                            Event.name,
                            Event.discipline)
                    .join(Result, src=Points)
                    .join(Person, src=Result)
                    .join(Race, src=Result)
                    .join(Event, src=Race)
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
            obra = get_obra_data(point.result.person, point.result.race.date)
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
    if race.date >= SCHEDULE_2019_DATE:
        schedule = SCHEDULE_2019
    else:
        schedule = SCHEDULE_2018

    if event_discipline in schedule:
        if field in schedule[event_discipline]:
            field_size_list = schedule[event_discipline][field]
        else:
            field_size_list = schedule[event_discipline]['open']

        for field_size in field_size_list:
            if race.starters >= field_size['min'] and race.starters <= field_size['max']:
                return field_size['points']
    else:
        logger.warn('No points schedule for event_discipline={} field={} starters={} date={}'.format(event_discipline, field, race.starters, race.date))

    return []


def needs_upgrade(person, upgrade_discipline, points_sum, categories, cat_points):
    """
    Determine if the rider needs an upgrade for this discipline
    """
    category = max(categories) - 1

    if upgrade_discipline in UPGRADES and category in UPGRADES[upgrade_discipline]:
        if 'podiums' in UPGRADES[upgrade_discipline][category]:
            # FIXME - also need to check field size and gender
            podiums = UPGRADES[upgrade_discipline][category]['podiums']
            podium_races = [p for p in cat_points if safe_int(p.place) <= 3]
            if len(podium_races) >= podiums:
                return True
            else:
                return False
        else:
            if category == 0:
                return False
            max_points = UPGRADES[upgrade_discipline][category]['max']
            return points_sum >= max_points
    else:
        logger.warn('No upgrade schedule for upgrade_discipline={} category={}'.format(upgrade_discipline, category))

    return False


def can_upgrade(upgrade_discipline, points_sum, category, cat_points, check_min_races=False):
    """
    Determine if the rider can upgrade to a given category, based on their current points and race count
    """
    if upgrade_discipline in UPGRADES and category in UPGRADES[upgrade_discipline]:
        if 'podiums' in UPGRADES[upgrade_discipline][category]:
            return category > 0
        else:
            min_points = UPGRADES[upgrade_discipline][category].get('min')
            min_races = UPGRADES[upgrade_discipline][category].get('races')
            logger.debug('Checking upgrade_discipline={} points_sum={} category={} num_races={} min_points={} min_races={}'.format(
                upgrade_discipline, points_sum, category, len(cat_points), min_points, min_races))
            if check_min_races and min_races and len(cat_points) >= min_races:
                return True
            elif points_sum >= min_points:
                return True
            else:
                return False
    else:
        logger.warn('No upgrade schedule for upgrade_discipline={} category={}'.format(upgrade_discipline, category))

    return True


def get_obra_data(person, date):
    """
    Try to get a snapshot of OBRA data from on or before the given date.
    If we have data from on or before the requested date, use that.
    If we have data from some other newer date, use that.
    If we don't have any data at all, get some.
    """
    if person.obra.where(ObraPersonSnapshot.date <= date).count():
        query = person.obra.order_by(ObraPersonSnapshot.date.desc()).where(ObraPersonSnapshot.date <= date)
    elif person.obra.count():
        query = person.obra.order_by(ObraPersonSnapshot.date.asc())
    else:
        scrape_person(person)
        query = person.obra

    data = query.first()
    logger.info('OBRA Data: data requested={} returned={} for person={}'.format(date, data.date, person.id))
    return data


def safe_int(value):
    try:
        return int(value)
    except Exception:
        return 999


def expire_points(points, race_date):
    """
    Calculate the sum of all points earned more than one year (plus a one-week grace period) ago.
    Modify the passed list by removing these expired points, and return the previously calculated sum.
    """
    expired_points = sum(int(p.value) for p in points if (race_date - p.date).days > 372)
    points[:] = [p for p in points if (race_date - p.date).days <= 372]
    return expired_points
