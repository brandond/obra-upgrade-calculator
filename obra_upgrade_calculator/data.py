#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from datetime import date

CATEGORY_RE = re.compile(r'(?:^| )(beginner|novice|[a-c]|[1-5](?:/[1-5])*)(?: |$)', flags=re.I)
AGE_RANGE_RE = re.compile(r'([7-9]|1[0-9])(-([7-9]|1[0-9]))?')
NAME_RE = re.compile("^[a-z.'-]+", flags=re.I)
NUMBER_RE = re.compile("[0-9]+|dnf|dq", flags=re.I)

DISCIPLINE_RE_MAP = {  # patterns within each discipline are ordered by precedence
    'road': [
        ['', re.compile('combined', flags=re.I)],
        ['circuit', re.compile('circuit|barton|dirty circles|kings valley|montinore|'
                               'piece of cake|tabor|(monday|tuesday)( night)? pir|'
                               'champion( thursday|ship raceway)|banana belt', flags=re.I)],
        ['criterium', re.compile('crit', flags=re.I)],
        ['time_trial', re.compile(' tt|time trial|climb|uphill|revenge of the disc', flags=re.I)],
        ['tour', re.compile('tour|fondo|epic|duro|stage', flags=re.I)],
    ],
}

# Points schedule changed effective 2019-08-31
SCHEDULE_2019_DATE = date(2019, 8, 31)
SCHEDULE_2019 = {
    'cyclocross': {
        'open': [
            {'min': 10, 'max': 25,  'points': [3,  2, 1]},
            {'min': 26, 'max': 40,  'points': [5,  4, 3, 2, 1]},
            {'min': 41, 'max': 75,  'points': [7,  6, 5, 4, 3, 2, 1]},
            {'min': 76, 'max': 999, 'points': [10, 8, 7, 5, 4, 3, 2, 1]},
        ],
        'women': [
            {'min':  6, 'max': 15,  'points': [3,  2, 1]},
            {'min': 16, 'max': 25,  'points': [5,  4, 3, 2, 1]},
            {'min': 26, 'max': 60,  'points': [7,  6, 5, 4, 3, 2, 1]},
            {'min': 61, 'max': 999, 'points': [10, 8, 7, 5, 4, 3, 2, 1]},
        ],
    },
    'circuit': {
        'open': [
            {'min':  5, 'max': 10,  'points': [3, 2, 1]},
            {'min': 11, 'max': 20,  'points': [4, 3, 2, 1]},
            {'min': 21, 'max': 49,  'points': [5, 4, 3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [7, 5, 4, 3, 2, 1]},
        ],
    },
    'criterium': {  # same as circuit
        'open': [
            {'min':  5, 'max': 10,  'points': [3, 2, 1]},
            {'min': 11, 'max': 20,  'points': [4, 3, 2, 1]},
            {'min': 21, 'max': 49,  'points': [5, 4, 3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [7, 5, 4, 3, 2, 1]},
        ],
    },
    'road': {
        'open': [
            {'min':  5, 'max': 10,  'points': [3,  2, 1]},
            {'min': 11, 'max': 20,  'points': [7,  5, 4, 3, 2, 1]},
            {'min': 21, 'max': 49,  'points': [8,  6, 5, 4, 3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [10, 8, 7, 6, 5, 4, 3, 2, 1]},
        ],
    },
    'tour': {
        'open': [
            {'min': 10, 'max': 19,  'points': [5,  3,  2,  1]},
            {'min': 20, 'max': 35,  'points': [7,  5,  3,  2,  1]},
            {'min': 36, 'max': 49,  'points': [10, 8,  6,  5,  4,  3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [20, 18, 16, 14, 12, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]},
        ],
    },
}

SCHEDULE_2018 = {
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
    },
    'circuit': {
        'open': [
            {'min':  5, 'max': 10,  'points': [3, 2, 1]},
            {'min': 11, 'max': 20,  'points': [4, 3, 2, 1]},
            {'min': 21, 'max': 49,  'points': [5, 4, 3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [7, 5, 4, 3, 2, 1]},
        ],
    },
    'criterium': {  # same as circuit
        'open': [
            {'min':  5, 'max': 10,  'points': [3, 2, 1]},
            {'min': 11, 'max': 20,  'points': [4, 3, 2, 1]},
            {'min': 21, 'max': 49,  'points': [5, 4, 3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [7, 5, 4, 3, 2, 1]},
        ],
    },
    'road': {
        'open': [
            {'min':  5, 'max': 10,  'points': [3,  2, 1]},
            {'min': 11, 'max': 20,  'points': [7,  5, 4, 3, 2, 1]},
            {'min': 21, 'max': 49,  'points': [8,  6, 5, 4, 3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [10, 8, 7, 6, 5, 4, 3, 2, 1]},
        ],
    },
    'tour': {
        'open': [
            {'min': 10, 'max': 19,  'points': [5,  3,  2,  1]},
            {'min': 20, 'max': 35,  'points': [7,  5,  3,  2,  1]},
            {'min': 36, 'max': 49,  'points': [10, 8,  6,  5,  4,  3, 2, 1]},
            {'min': 50, 'max': 999, 'points': [20, 18, 16, 14, 12, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]},
        ],
    },
}

# Minimum points necesary to upgrade to this field.
# Maximum points after which you are mandatorily upgraded.
UPGRADES = {
    'cyclocross': {
        4: {'min': 0,  'max': 20},
        3: {'min': 0,  'max': 20},
        2: {'min': 20, 'max': 20},
        1: {'min': 20, 'max': 35},
    },
    'mountain_bike': {
        3: {'podiums': 0},
        2: {'podiums': 3},
        1: {'podiums': 3},
        0: {'podiums': 5},
    },
    'track': {
        4: {'min': 0,  'races': 4},
        3: {'min': 20, 'races': 5},
        2: {'min': 25, 'races': 5},
        1: {'min': 30, 'races': 5},
    },
    'road': {
        4: {'min': 15, 'max': 25, 'races': 10},
        3: {'min': 20, 'max': 30, 'races': 25},
        2: {'min': 25, 'max': 40},
        1: {'min': 30, 'max': 50},
    },
}

# Map event disciplines to upgrade schedules
DISCIPLINE_MAP = {
    'road':          ['road', 'circuit', 'criterium', 'gravel', 'time_trial', 'tour'],
    'mountain_bike': ['mountain_bike', 'downhill', 'super_d', 'short_track'],
    'cyclocross':    ['cyclocross'],
    'track':         ['track'],
}
