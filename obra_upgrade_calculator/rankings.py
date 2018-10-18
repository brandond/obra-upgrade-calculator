#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
import logging
logging.basicConfig(level='DEBUG')

from datetime import date, timedelta
import ujson as json

from peewee import JOIN, fn, prefetch

from .models import Event, Race, Result, Person, Rank, Quality, Points

logger = logging.getLogger(__name__)


def get_rank(person, end_date=None):
    if not end_date:
        end_date = date.today()
    start_date = end_date - timedelta(days=365)

    ranks = [600] * 5
    query = (Rank.select(fn.json_group_array(Rank.value, coerce=False))
                 .join(Result)
                 .join(Race)
                 .where(Result.person_id == person.id)
                 .where(Race.date >= start_date)
                 .where(Race.date <= end_date)
                 .order_by(Rank.value.asc())
                 .limit(5))
    ranks += json.loads(query.scalar())
    return sum(sorted(ranks)[:5]) / 5

def calculate_race_ranks():
    Rank.delete().execute()
    Quality.delete().execute()
    races = (Race.select(Race,
                         Event.name)
                 .join(Event)
                 .where(Event.type == 'cyclocross')
#                 .where(Race.categories.length() != 0)
                 .where((Race.categories.length() != 0) | 
                        (Race.name ** '%single%'))
                 .order_by(Race.date.asc()))

    for race in races:
        """
        1. From top 10 finishers, get top 5 ranked riders; average ranks and multiply by 0.9
        2. Average all ranked finishers and multiply by 0.9
        3. Store lesser of 1 or 2 as quality.value
        4. Store (((Average all ranked finishers) - (quality.value)) * 2) / (race.results.count() - 1) as quality.points_per_place
        5. For each result, store quality.value + ((result.place - 1) * quality.points_per_place) as rank.value
        """
        logger.info('{} {}: {}'.format(race.date, race.event.name, race.name))
        results = (race.results.select(Result.id,
                                       Result.place,
                                       Person.id)
                               .join(Person)
                               .where(~(Result.place ** ('DN%')))
                               .where(Result.place.cast('integer') > 0)
                               .order_by(Result.place.cast('integer').asc()))

        if results.count() <= 2:
            continue

        ranks = [600] * 5
        ranks += [get_rank(result.person, race.date) for result in results.limit(10)]
        min_rank = min(ranks)
        top_average = sum(sorted(ranks)[:5]) / 5

        ranks = [get_rank(result.person, race.date) for result in results]
        all_average = sum(ranks) / len(ranks)
        value = (all_average if all_average < top_average and all_average > min_rank else top_average) * 0.9
        per_place = ((all_average - value) * 2) / (results.count() - 1)

        logger.info('\tAverage of top 5: {}'.format(top_average))
        logger.info('\tAverage of field: {}'.format(all_average))
        logger.info('\tBest rider rank:  {}'.format(min_rank))
        logger.info('\tQuality value:    {}'.format(value))
        logger.info('\tPoints per Place: {}'.format(per_place))
        Quality.create(race=race, value=value, points_per_place=per_place)

        for result in results:
            rank = value + ((int(result.place) - 1) * per_place)
            rank = int(rank) if rank <= 590 else 590
            Rank.create(result=result, value=rank)
            logger.debug('\t\t{}: {} - {}'.format(result.person.id, result.place, rank))

def dump_ranks():
    start_date = date.today() - timedelta(days=365)
    people = (Person.select(Person)
                    .join(Result)
                    .join(Race)
                    .switch(Result)
                    .join(Rank)
                    .where(Race.date >= start_date)
                    .group_by(Person)
                    .order_by(Person.last_name.collate('NOCASE').asc(),
                              Person.first_name.collate('NOCASE').asc()))

    ranks = [(person, get_rank(person)) for person in people]
    i = 0
    last_rank = 0
    print('Place| Cat   | Name                    : Rank points')
    for (person, rank) in sorted(ranks, key=lambda r: r[1]):
        if last_rank != rank:
            last_rank = rank
            i += 1
        cat = (Points.select(Points.sum_categories)
                     .join(Result)
                     .join(Race)
                     .where(Result.person == person)
                     .order_by(Race.date.desc())
                     .limit(1)
                     .scalar())
        cat = '/'.join(str(c) for c in cat) if cat else 'SS'
        name = '{}, {}'.format(person.last_name, person.first_name)
        print('{0:<4} | {1:<5} | {2:<24}: {3:>3} points'.format(i, cat, name, rank))


def dump_rank_history(person):
    start_date = date.today() - timedelta(days=365)
    ranks = (Rank.select(Rank,
                         Result,
                         Person,
                         Race)
                 .join(Result)
                 .join(Person)
                 .switch(Result)
                 .join(Race)
                 .where(Result.person == person)
                 .where(Race.date >= start_date)
                 .order_by(Race.date.asc()))
    print('{}, {}: {}'.format(person.last_name, person.first_name, get_rank(person)))
    for rank in ranks:
        print('\t{0:>3} points | {1} - {2:<2} in {3}'.format(rank.value, rank.result.race.date, rank.result.place, rank.result.race.name))

if __name__ == '__main__':
    calculate_race_ranks()
    dump_ranks()
#     person = Person.get(Person.first_name == sys.argv[1], Person.last_name == sys.argv[2])
#     dump_rank_history(person)

