#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from collections import defaultdict
from datetime import date, timedelta

from peewee import fn

from .data import DISCIPLINE_MAP
from .models import Event, Quality, Race, Rank, Result

try:
    import ujson as json
except ImportError:
    import json

logger = logging.getLogger(__name__)


def get_ranks(upgrade_discipline, end_date=None, person_ids=[]):
    """
    Return a dict of everyone's rank for this discipline as of a given date
    """
    if not end_date:
        end_date = date.today()
    start_date = end_date - timedelta(days=365)

    default_ranks = [600] * 5
    query = (Rank.select(Result.person_id, fn.json_group_array(Rank.value).python_value(json.loads))
                 .join(Result, src=Rank)
                 .join(Race, src=Result)
                 .join(Event, src=Race)
                 .where(Race.date >= start_date)
                 .where(Race.date < end_date)
                 .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                 .group_by(Result.person_id))

    if person_ids:
        query = query.where(Result.person_id << person_ids)

    logger.debug('Got {} People in {} between {} and {}'.format(query.count(), upgrade_discipline, start_date, end_date))
    return defaultdict(lambda: 600, ((person_id, sum(sorted(default_ranks + ranks)[:5]) / 5) for person_id, ranks in query.tuples()))


def calculate_race_ranks(upgrade_discipline, incremental=False):
    # Delete all Rank and Quality data for this discipline and recalc from scratch

    if not incremental:
        (Rank.delete()
             .where(Rank.result_id << (Result.select(Result.id)
                                             .join(Race, src=Result)
                                             .join(Event, src=Race)
                                             .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])))
             .execute())

        (Quality.delete()
                .where(Quality.race_id << (Race.select(Race.id)
                                               .join(Event, src=Race)
                                               .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])))
                .execute())

    prev_race = Race()
    races = (Race.select(Race, Event)
                 .join(Event, src=Race)
                 .where(Race.id.not_in(Quality.select(fn.DISTINCT(Race.id))
                                              .join(Race, src=Quality)
                                              .join(Event, src=Race)
                                              .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])))
                 .where(Event.discipline << DISCIPLINE_MAP[upgrade_discipline])
                 .where(Race.categories.length() != 0)  # | (Race.name ** '%single%'))  #<-- uncomment to include SS
                 .order_by(Race.date.asc(), Race.created.asc()))

    for race in races:
        """
        1. From top 10 finishers, get top 5 ranked riders; average ranks and multiply by 0.9
        2. Average all ranked finishers and multiply by 0.9
        3. If 2 is less than 1, and 2 is greater the lowest rank in the top 10, then use 2 as quality.value, else use 1
        4. Store (((Average all ranked finishers) - (quality.value)) * 2) / (race.results.count() - 1) as quality.points_per_place
        5. For each result, store quality.value + ((result.place - 1) * quality.points_per_place) as rank.value
        """
        logger.info('Processing Race: [{}]{}: [{}]{} on {}'.format(race.event.id, race.event.name, race.id, race.name, race.date))

        results = (race.results.select()
                               .where(~(Result.place.contains('dns')))
                               .where(~(Result.place.contains('dnf')))
                               .order_by(Result.id.asc()))
        finishers = results.count()

        if finishers <= 2:
            logger.debug('Insufficient finishers: {}'.format(finishers))
            Quality.create(race=race, value=0, points_per_place=0)
            continue

        # Bulk cache everyone's ranks for this date so we don't have to re-query them all one by one
        if prev_race.date != race.date:
            people = get_ranks(upgrade_discipline, race.date)

        ranks = [600] * 5
        ranks += [people[result.person_id] for result in results.limit(10)]
        min_rank = min(ranks)
        top_average = sum(sorted(ranks)[:5]) / 5

        ranks = [people[result.person_id] for result in results]
        all_average = sum(ranks) / len(ranks)
        value = (all_average if all_average < top_average and all_average > min_rank else top_average) * 0.9
        per_place = ((all_average - value) * 2) / (finishers - 1)

        logger.info('\tStart/Finishers:  {}/{}'.format(race.starters, finishers))
        logger.info('\tAverage of top 5: {}'.format(top_average))
        logger.info('\tAverage of field: {}'.format(all_average))
        logger.info('\tBest top 10 rank: {}'.format(min_rank))
        logger.info('\tQuality value:    {}'.format(value))
        logger.info('\tPoints per Place: {}'.format(per_place))
        Quality.create(race=race, value=value, points_per_place=per_place)

        insert_ranks = []
        for zplace, result in enumerate(results):
            rank = value + (zplace * per_place)
            rank = rank if rank <= 590 else 590
            insert_ranks.append((result, rank))

        Rank.insert_many(insert_ranks, fields=[Rank.result, Rank.value]).on_conflict_replace().execute()
        prev_race = race
