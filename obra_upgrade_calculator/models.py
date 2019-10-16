#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from os.path import expanduser

from peewee import AutoField, BooleanField, Model
from playhouse.apsw_ext import (APSWDatabase, CharField, DateField,
                                DateTimeField, ForeignKeyField, IntegerField)
from playhouse.sqlite_ext import JSONField

db = APSWDatabase(expanduser('~/.obra.sqlite3'),
                  pragmas=(('foreign_keys', 'on'),
                           ('page_size', 1024 * 4),
                           ('cache_size', 10000),
                           ('auto_vacuum', 'NONE'),
                           ('journal_mode', 'TRUNCATE'),
                           ('locking_mode', 'NORMAL'),
                           ('synchronous', 'NORMAL')))

logger = logging.getLogger(__name__)
logger.info('Using local database {} at {}'.format(db, db.database))


class ObraModel(Model):
    class Meta:
        database = db
        without_rowid = True
        only_save_dirty = True


class Series(ObraModel):
    """
    An OBRA race series spanning multiple events over more than one day.
    """
    id = IntegerField(verbose_name='Series ID', primary_key=True)
    name = CharField(verbose_name='Series Name')
    year = IntegerField(verbose_name='Series Year')
    dates = CharField(verbose_name='Series Months/Days')


class Event(ObraModel):
    """
    A single race day - may be standalone or part of a series.
    """
    id = IntegerField(verbose_name='Event ID', primary_key=True)
    name = CharField(verbose_name='Event Name')
    discipline = CharField(verbose_name='Event Discipline', index=True)
    year = IntegerField(verbose_name='Event Year')
    date = CharField(verbose_name='Event Month/Day')
    series = ForeignKeyField(verbose_name='Event Series', model=Series, backref='events', null=True)
    parent = ForeignKeyField(verbose_name='Child Events', model='self', backref='children', null=True)
    ignore = BooleanField(verbose_name='Ignore/Hide Event', default=False)

    @property
    def discipline_title(self):
        return self.discipline.replace('_', ' ').title()


class Race(ObraModel):
    """
    A single race at an event, with one or more results.
    """
    id = IntegerField(verbose_name='Race ID', primary_key=True)
    name = CharField(verbose_name='Race Name')
    date = DateField(verbose_name='Race Date')
    categories = JSONField(verbose_name='Race Categories')
    starters = IntegerField(verbose_name='Race Starting Field Size', default=0)
    created = DateTimeField(verbose_name='Results Created')
    updated = DateTimeField(verbose_name='Results Updated')
    event = ForeignKeyField(verbose_name='Race Event', model=Event, backref='races')


class Person(ObraModel):
    """
    A person who participated in a race.
    """
    id = IntegerField(verbose_name='Person ID', primary_key=True)
    first_name = CharField(verbose_name='First Name')
    last_name = CharField(verbose_name='Last Name')
    team_name = CharField(verbose_name='Team Name', default='')


class ObraPersonSnapshot(ObraModel):
    """
    A point in time record of OBRA member data.
    The OBRA website doesn't make historical data available, so we store a timestamped
    copy every time we do a lookup. Doesn't help with really old upgrades, but it should
    be useful going forward.
    """
    id = AutoField(verbose_name='Scrape ID', primary_key=True)
    date = DateField(verbose_name='Scrape Date')
    person = ForeignKeyField(verbose_name='Person', model=Person, backref='obra')
    license = IntegerField(verbose_name='License', null=True)
    mtb_category = IntegerField(verbose_name='MTB Category', default=3)
    dh_category = IntegerField(verbose_name='DH Category', default=3)
    ccx_category = IntegerField(verbose_name='CX Category', default=5)
    road_category = IntegerField(verbose_name='Road Category', default=5)
    track_category = IntegerField(verbose_name='Track Category', default=5)

    class Meta:
        indexes = (
            (('date', 'person'), True),
        )

    def category_for_discipline(self, discipline):
        discipline = discipline.replace('mountain_bike', 'mtb')
        discipline = discipline.replace('short_track', 'mtb')
        discipline = discipline.replace('cyclocross', 'ccx')
        discipline = discipline.replace('criterium', 'road')
        discipline = discipline.replace('time_trial', 'road')
        discipline = discipline.replace('circuit', 'road')
        discipline = discipline.replace('gran_fondo', 'road')
        discipline = discipline.replace('gravel', 'road')
        discipline = discipline.replace('tour', 'road')
        discipline = discipline.replace('downhill', 'dh')
        discipline = discipline.replace('super_d', 'dh')
        return getattr(self, discipline + '_category')


class Result(ObraModel):
    """
    An individual race result - a Person's place in a specific Race.
    """
    id = IntegerField(verbose_name='Result ID', primary_key=True)
    race = ForeignKeyField(verbose_name='Result Race', model=Race, backref='results')
    person = ForeignKeyField(verbose_name='Result Person', model=Person, backref='results', null=True)
    place = CharField(verbose_name='Place', index=True)
    time = IntegerField(verbose_name='Time', null=True)
    laps = IntegerField(verbose_name='Laps', null=True)


class Points(ObraModel):
    """
    Points toward a category upgrade - awarded for a good Result in a Race of a specific size.
    """
    result = ForeignKeyField(verbose_name='Result awarding Upgrade Points', model=Result, backref='points', primary_key=True)
    value = CharField(verbose_name='Points Earned for Result', default='0')
    notes = CharField(verbose_name='Notes', default='')
    needs_upgrade = BooleanField(verbose_name='Needs Upgrade', default=False)
    upgrade_confirmation = ForeignKeyField(verbose_name='Member Data Confirming Upgrade', model=ObraPersonSnapshot, backref='points', null=True)
    sum_value = IntegerField(verbose_name='Current Points Sum', default=0)
    sum_categories = JSONField(verbose_name='Current Category', default=[])


class Rank(ObraModel):
    """
    Rank points associated with a Result
    """
    result = ForeignKeyField(verbose_name='Rank from Result', model=Result, backref='rank', primary_key=True)
    value = IntegerField(verbose_name='Rank for Place')


class Quality(ObraModel):
    """
    Race Quality figures for a Race
    """
    race = ForeignKeyField(verbose_name='Quality Race', model=Race, backref='quality')
    value = IntegerField(verbose_name='Race Quality')
    points_per_place = IntegerField(verbose_name='Points per Place')


with db.connection_context():
    db.create_tables([Series, Event, Race, Person, ObraPersonSnapshot, Result, Points, Rank, Quality], fail_silently=True)

    try:
        db.execute_sql('VACUUM')
    except Exception as e:
        logger.warn('Failed to vacuum database: {}'.format(e))
