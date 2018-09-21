
import logging
from os.path import expanduser

from peewee import Model
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
    created = DateTimeField(verbose_name='Results Created')
    updated = DateTimeField(verbose_name='Results Updated')
    event = ForeignKeyField(verbose_name='Race Event', model=Event, backref='races')

    class Meta:
        database = db
        only_save_dirty = True


class Person(Model):
    id = IntegerField(verbose_name='Person ID', primary_key=True)
    obra_id = IntegerField(verbose_name='OBRA ID', null=True)
    first_name = CharField(verbose_name='First Name')
    last_name = CharField(verbose_name='Last Name')

    class Meta:
        database = db
        only_save_dirty = True


class Result(Model):
    id = IntegerField(verbose_name='Result ID', primary_key=True)
    race = ForeignKeyField(verbose_name='Result Race', model=Race, backref='results')
    person = ForeignKeyField(verbose_name='Result Person', model=Person, backref='results', null=True)
    place = CharField(verbose_name='Place')

    class Meta:
        database = db
        only_save_dirty = True


class Points(Model):
    person = ForeignKeyField(verbose_name='Points Person', model=Person, null=True)
    race = ForeignKeyField(verbose_name='Points from Race', model=Race)
    categories = JSONField(verbose_name='Points in Categories')
    place = IntegerField(verbose_name='Place')
    starters = IntegerField(verbose_name='Starting Field Size')
    points = IntegerField(verbose_name='Points for Place')

    class Meta:
        database = db
        only_save_dirty = True
        indexes = (
            (('person', 'race'), True),
        )


db.connect()
Series.create_table(fail_silently=True)
Event.create_table(fail_silently=True)
Race.create_table(fail_silently=True)
Person.create_table(fail_silently=True)
Result.create_table(fail_silently=True)
Points.create_table(fail_silently=True)
logging.info('Using local database {} at {}'.format(db, db.database))
