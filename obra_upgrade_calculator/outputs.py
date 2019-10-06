#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import io
from datetime import datetime
from textwrap import dedent

HTML_HEADER = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>OBRA: Upgrade Points for {0}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="stylesheet" media="all"
        href="https://obra.org/assets/application-dd5048284cc6fef6c0b95b453b0f58065ba4863b794b5875262e107c9b39c9bc.css" />
        <link rel="stylesheet" media="screen"
        href="https://obra.org/assets/registration_engine/application-d124af1d06dfaa2cf725f48e0237bfe328b1bffb680a16c8853a40978279c767.css" />
        <link rel="shortcut icon" type="image/x-icon"
        href="https://obra.org/assets/favicon-d8d3df3a13d3a80d51fffc68da9d6f49ba553932a8fe618068984dcb514363c3.ico" />
        <link rel="apple-touch-icon" type="image/png"
        href="https://obra.org/assets/apple-touch-icon-8790d9d360b222bef07545037f2878ed5918ee49ebda1e0913a222ff6872c04e.png" />
    </head>
    <body>
      <div class="container page-nav">
        <div class="content">
          <h2>Upgrade Points for {0}</h2>
          <div class="row event_info">
            <a href="upgrades.csv">Download Raw CSV</a>
          </div>
          <p class="created_updated">Updated {1}</p>
    <!-- Start Content -->'''

HTML_UPGRADES_HEADER = '''
    <!-- Start Upgrades -->
          <div class="event_info">
            <h4 class="race">Upgrades Due</h4>
            <table class="base table-striped event_races">
              <thead>
                <tr>
                  <th class="race">Category</th>
                  <th class="race">Name</th>
                  <th class="points_total">Total Pts</th>
                  <th class="date pull-right">Date</th>
                </tr>
              </thead>
              <tbody>'''

HTML_UPGRADE = '''
    <!-- Upgrade -->
                <tr>
                  <td class="race">{sum_categories}</td>
                  <td class="race">
                    <a href="#person_{point.result.person.id}">
                        {point.result.person.first_name} {point.result.person.last_name}
                      </a>
                  </td>
                  <td class="points_total">{point.sum_value}</td>
                  <td class="date">{point.last_date}</td>
                </tr>'''

HTML_UPGRADES_FOOTER = '''
    <!-- End Upgrades -->
              </tbody>
            </table>
          </div>'''

HTML_PERSON_HEADER = '''
    <!-- Start Person -->
          <h3 class="race" id="person_{0.id}"><a href="#person_{0.id}">{0.first_name} {0.last_name}</a></h3>
          <table class="base table table-striped results">
            <thead>
              <tr>
                <th class="place"></th>
                <th class="points hidden-xs">Points</th>
                <th class="points_total">Total Pts</th>
                <th class="discipline hidden-xs">Discipline</th>
                <th class="event">Event</th>
                <th class="category">Race</th>
                <th class="place">Category</th>
                <th class="date hidden-xs">Date</th>
                <th class="notes">Notes</th>
              </tr>
            </thead>
            <tbody>'''

HTML_POINT = '''
    <!-- Point -->
              <tr>
                <td class="place">{point.result.place}</td>
                <td class="points hidden-xs">{point.value}</td>
                <td class="points_total">{point.sum_value}</td>
                <td class="discipline hidden-xs">{point.result.race.event.discipline_title}</td>
                <td class="event">
                  <a href="https://obra.org/events/{point.result.race.event.id}/results#race_{point.result.race.id}">
                    {point.result.race.event.name}
                  </a>
                </td>
                <td class="category">{point.result.race.name}</td>
                <td class="place">{sum_categories}</td>
                <td class="date hidden-xs">{point.result.race.date}</td>
                <td class="notes text-nowrap">{point.notes}</td>
              </tr>'''

HTML_PERSON_FOOTER = '''
    <!-- End Person -->
            </tbody>
          </table>'''

HTML_FOOTER = '''
    <!-- End Content -->
        </div>
      </div>
      <div class="container page-nav">
        <footer>
          <ul>
            <li><a class="link" href="https://github.com/brandond/obra-upgrade-calculator/">OBRA Upgrade Calculator</a></li>
          </ul>
        </footer>
      </div>
    </body>
    </html>'''


class OutputBase(object):
    def __init__(self, discipline, path='/dev/stdout'):
        self.discipline = discipline
        self.output = io.TextIOWrapper(io.open(path, 'wb'))

    def __enter__(self):
        if hasattr(self, 'header'):
            self.header()
        return self

    def start_upgrades(self):
        """Called at the start of the Upgrades block"""
        pass

    def upgrade(self, point):
        """Called to print a single person who needs an upgrade"""
        pass

    def end_upgrades(self):
        """Called at the end of the Upgrades block"""
        pass

    def start_person(self, person):
        """Called at the start of each Person"""
        pass

    def point(self, point):
        """Called to print a single point"""
        pass

    def end_person(self, person, final=False):
        """Called at the end of each Person"""
        pass

    def __exit__(self, type, value, traceback):
        if hasattr(self, 'footer'):
            self.footer()
        return None


class TextOutput(OutputBase):
    def header(self):
        self.output.write('--- Upgrade Points for {} ---\n\n'.format(
            self.discipline.capitalize()))

    def point(self, point):
        if point.notes:
            point.notes = '*** {} ***'.format(point.notes)
        self.output.write('{0:<24} | {1:>2} points in Cat {2:<3} | {3:>2} for {4:>2}/{5:<2} at [{6}]{7}: {8} on {9}  {10}\n'.format(
            ', '.join([point.result.person.last_name, point.result.person.first_name]),
            point.sum_value,
            '/'.join(str(c) for c in point.sum_categories),
            point.value,
            point.result.place,
            point.result.race.starters,
            point.result.race.event.discipline_title,
            point.result.race.event.name,
            point.result.race.name,
            point.result.race.date,
            point.notes))

    def end_person(self, person, final=False):
        if not final:
            self.output.write('-------------------------|----------------------|---------------------\n')


class HtmlOutput(OutputBase):
    def header(self):
        self.output.write(dedent(HTML_HEADER).format(self.discipline.capitalize(), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def start_upgrades(self):
        self.output.write(dedent(HTML_UPGRADES_HEADER))

    def upgrade(self, point):
        self.output.write(dedent(HTML_UPGRADE).format(
            point=point,
            sum_categories='/'.join(str(c) for c in point.sum_categories)))

    def end_upgrades(self):
        self.output.write(dedent(HTML_UPGRADES_FOOTER))

    def start_person(self, person):
        self.output.write(dedent(HTML_PERSON_HEADER).format(person))

    def point(self, point):
        self.output.write(dedent(HTML_POINT).format(
            point=point,
            sum_categories='/'.join(str(c) for c in point.sum_categories)))

    def end_person(self, person, final=False):
        self.output.write(dedent(HTML_PERSON_FOOTER))

    def footer(self):
        self.output.write(dedent(HTML_FOOTER))


class JsonOutput(OutputBase):
    def header(self):
        self.output.write('{\n')
        self.output.write('  "discipline": "{}",\n'.format(self.discipline))
        self.output.write('  "people": [\n')

    def start_person(self, person):
        self.point_buffer = ''
        self.output.write('    {\n')
        self.output.write('      "first_name": "{}",\n'.format(person.first_name))
        self.output.write('      "last_name": "{}",\n'.format(person.last_name))
        self.output.write('      "points": [\n')

    def point(self, point):
        if self.point_buffer:
            self.output.write(self.point_buffer + ',\n')

        self.point_buffer = '        { '
        self.point_buffer += '"place": {}, "starters": {}, "points": {}, "point_total": {}, '.format(
            point.result.place,
            point.result.race.starters,
            point.value,
            point.sum_value)
        self.point_buffer += '"category": "{}", "discipline": "{}", "event": "{}", "race": "{}", "date": "{}", "notes": "{}" '.format(
            '/'.join(str(c) for c in point.sum_categories),
            point.result.race.event.discipline,
            point.result.race.event.name,
            point.result.race.name,
            point.result.race.date,
            point.notes)
        self.point_buffer += '}'

    def end_person(self, person, final=False):
        self.output.write(self.point_buffer + '\n')
        self.output.write('      ]\n')
        self.output.write('    }' + ('\n' if final else ',\n'))

    def footer(self):
        self.output.write('  ]\n')
        self.output.write('}\n')


class CsvOutput(OutputBase):
    def header(self):
        self.output.write('Place, Starters, Points, Points Total, First Name, Last Name, Category, Discipline, Event, Race, Date, Notes\n')

    def point(self, point):
        self.output.write('{0},{1:>2},{2:>2},{3:>2},"{4}"\t,"{5}"\t,"{6}"\t,"{7}"\t,"{8}"\t,"{9}"\t,{10},"{11}"\n'.format(
            point.result.place,
            point.result.race.starters,
            point.value,
            point.sum_value,
            point.result.person.first_name,
            point.result.person.last_name,
            '/'.join(str(c) for c in point.sum_categories),
            point.result.race.event.discipline_title,
            point.result.race.event.name,
            point.result.race.name,
            point.result.race.date,
            point.notes))


OUTPUT_MAP = {'text': TextOutput,
              'html': HtmlOutput,
              'json': JsonOutput,
              'csv': CsvOutput,
              'null': OutputBase,
              }


def get_writer(output_format, *args, **kwargs):
    if output_format in OUTPUT_MAP:
        return OUTPUT_MAP[output_format](*args, **kwargs)
    else:
        raise NotImplementedError()
