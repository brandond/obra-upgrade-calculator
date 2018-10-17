#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from datetime import datetime, timedelta
from os.path import expanduser

from peewee import BooleanField, Model
from playhouse.apsw_ext import (APSWDatabase, CharField, DateField,
                                DateTimeField, ForeignKeyField, IntegerField)
from playhouse.sqlite_ext import JSONField

logger = logging.getLogger(__name__)
db = APSWDatabase(expanduser('~/.obra.sqlite3'),
                  pragmas=(('foreign_keys', 'on'),
                           ('page_size', 1024 * 4),
                           ('cache_size', 10000),
                           ('auto_vacuum', 'NONE'),
                           ('journal_mode', 'OFF'),
                           ('locking_mode', 'EXCLUSIVE'),
                           ('synchronous', 'OFF')))


class Series(Model):
    id = IntegerField(verbose_name='Series ID', primary_key=True)
    name = CharField(verbose_name='Series Name')
    type = CharField(verbose_name='Series Type')
    year = IntegerField(verbose_name='Series Year')
    dates = CharField(verbose_name='Series Months/Days')

    class Meta:
        database = db
        only_save_dirty = True


class Event(Model):
    id = IntegerField(verbose_name='Event ID', primary_key=True)
    name = CharField(verbose_name='Event Name')
    type = CharField(verbose_name='Event Type')
    year = IntegerField(verbose_name='Event Year')
    date = CharField(verbose_name='Event Month/Day')
    series = ForeignKeyField(verbose_name='Event Series', model=Series, backref='events', null=True)

    class Meta:
        database = db
        only_save_dirty = True


class Race(Model):
    id = IntegerField(verbose_name='Race ID', primary_key=True)
    name = CharField(verbose_name='Race Name')
    date = DateField(verbose_name='Race Date')
    categories = JSONField(verbose_name='Race Categories')
    created = DateTimeField(verbose_name='Results Created')
    updated = DateTimeField(verbose_name='Results Updated')
    event = ForeignKeyField(verbose_name='Race Event', model=Event, backref='races')

    class Meta:
        database = db
        only_save_dirty = True


class Person(Model):
    id = IntegerField(verbose_name='Person ID', primary_key=True)
    first_name = CharField(verbose_name='First Name')
    last_name = CharField(verbose_name='Last Name')

    class Meta:
        database = db
        only_save_dirty = True


class ObraPerson(Model):
    license = IntegerField(verbose_name='License', primary_key=True)
    person = ForeignKeyField(verbose_name='Person', model=Person, backref='obra')
    mtb_category = IntegerField(verbose_name='MTB Category', null=True)
    dh_category = IntegerField(verbose_name='DH Category', null=True)
    ccx_category = IntegerField(verbose_name='CX Category', null=True)
    road_category = IntegerField(verbose_name='Road Category', null=True)
    track_category = IntegerField(verbose_name='Track Category', null=True)
    updated = DateTimeField(verbose_name='Person Updated')

    class Meta:
        database = db
        only_save_dirty = True

    def category(self, event_type):
        event_type = event_type.replace('mountain_bike', 'mtb')
        event_type = event_type.replace('cyclocross', 'ccx')
        event_type = event_type.replace('downhill', 'dh')
        return getattr(self, event_type + '_category', None)

    def is_expired(self):
        return datetime.now() - self.updated >= timedelta(days=1)


class Result(Model):
    id = IntegerField(verbose_name='Result ID', primary_key=True)
    race = ForeignKeyField(verbose_name='Result Race', model=Race, backref='results')
    person = ForeignKeyField(verbose_name='Result Person', model=Person, backref='results', null=True)
    place = CharField(verbose_name='Place')

    class Meta:
        database = db
        only_save_dirty = True


class Points(Model):
    result = ForeignKeyField(verbose_name='Points from Result', model=Result, backref='points', primary_key=True)
    starters = CharField(verbose_name='Starting Field Size', default='?')
    value = CharField(verbose_name='Points for Place', default='0')
    notes = CharField(verbose_name='Notes', default='')
    needs_upgrade = BooleanField(verbose_name='Needs Upgrade', default=False)
    sum_value = IntegerField(verbose_name='Current Points', default=0)
    sum_categories = JSONField(verbose_name='Current Category', default=[])

    class Meta:
        database = db
        only_save_dirty = True


db.connect()
Series.create_table(fail_silently=True)
Event.create_table(fail_silently=True)
Race.create_table(fail_silently=True)
Person.create_table(fail_silently=True)
ObraPerson.create_table(fail_silently=True)
Result.create_table(fail_silently=True)
Points.create_table(fail_silently=True)
logging.debug('Using local database {} at {}'.format(db, db.database))
