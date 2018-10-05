from __future__ import unicode_literals

import io
from datetime import datetime
from textwrap import dedent

HTML_HEADER = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>OBRA: Upgrade Points for {0} since {1}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="stylesheet" media="all" href="https://obra.org/assets/application-367cae7561f3a791ddfdc0fe0224815ed8c86991ffa918002134a4b834ed8de7.css" />
        <link rel="stylesheet" media="screen" href="https://obra.org/assets/registration_engine/application-dbd90166764121e1ddeaf5c3adc56246b6fb9da8bf6e25e2656046d867bd8a4d.css" />
        <link rel="shortcut icon" type="image/x-icon" href="https://obra.org/assets/favicon-92e34b6df1439f05a9c06f664fc3e29fd040bc511eb6ffad1dbecbc1f004b2c8.ico" />
        <link rel="apple-touch-icon" type="image/png" href="https://obra.org/assets/apple-touch-icon-017f423f2e51e0838ead27ad35b6dd5d093e6d64a61c8d62bf633937b7df4d38.png" />
    </head>
    <body>
      <div class="container page-nav">
        <div class="content">
          <h2>Upgrade Points for {0}</h2>
          <div class="row event_info">
            Points Earned Since {1}<br>
            <a href="upgrades.csv">Download Raw CSV</a>
          </div>
          <p class="created_updated">Updated {2}</p>
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
                </tr>'''

HTML_UPGRADES_FOOTER = '''
    <!-- End Upgrades -->
              </tbody>
            </table>
          </div>'''

HTML_PERSON_HEADER = '''
    <!-- Start Person -->
          <h3 class="race" id="person_{0.id}"><a href="https://obra.org/people/{0.id}">{0.first_name} {0.last_name}</a></h3>
          <table class="base table table-striped results">
            <thead>
              <tr>
                <th class="place"></th>
                <th class="points hidden-xs">Points</th>
                <th class="points_total">Total Pts</th>
                <th class="event">Event</th>
                <th class="category">Category</th>
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
                <td class="event">
                  <a href="https://obra.org/events/{point.result.race.event.id}/results#race_{point.result.race.id}">
                    {point.result.race.event.name}
                  </a>
                </td>
                <td class="category">{point.result.race.name}</td>
                <td class="place">{sum_categories}</td>
                <td class="date hidden-xs">{point.result.race.date}</td>
                <td class="notes text-nowrap">{point.sum_notes}</td>
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
    def __init__(self, event_type, start_date, path='/dev/stdout'):
        self.event_type = event_type
        self.start_date = start_date
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
        self.output.write('--- Upgrade Points Earned In {} Races Since {} ---\n\n'.format(
            self.event_type.capitalize(), self.start_date.strftime('%Y-%m-%d')))

    def point(self, point):
        if point.sum_notes:
            point.sum_notes = '*** {} ***'.format(point.sum_notes)
        self.output.write('{0:<24s} | {1:>2d} points in Cat {2:<3s} | {3:>2d} for {4}/{5:<2d} at {6}: {7} on {8}  {9}\n'.format(
            ', '.join([point.result.person.last_name, point.result.person.first_name]),
            point.sum_value,
            '/'.join(str(c) for c in point.sum_categories),
            point.value,
            point.result.place,
            point.starters,
            point.result.race.event.name,
            point.result.race.name,
            point.result.race.date,
            point.sum_notes))

    def end_person(self, person, final=False):
        if not final:
            self.output.write('-------------------------|----------------------|---------------------\n')


class HtmlOutput(OutputBase):
    def header(self):
        self.output.write(dedent(HTML_HEADER).format(self.event_type.capitalize(), self.start_date.strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

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
        self.output.write('  "event_type": "{}",\n'.format(self.event_type))
        self.output.write('  "start_date": "{}",\n'.format(self.start_date))
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
            point.starters,
            point.value,
            point.sum_value)
        self.point_buffer += '"category": "{}", "event": "{}", "race": "{}", "date": "{}", "notes": "{}" '.format(
            '/'.join(str(c) for c in point.sum_categories),
            point.result.race.event.name,
            point.result.race.name,
            point.result.race.date,
            point.sum_notes)
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
        self.output.write('Place, Starters, Points, Points Total, First Name, Last Name, Category, Event, Race, Date, Notes\n')

    def point(self, point):
        self.output.write('{0},{1:>2d},{2:>2d},{3:>2d},"{4}"\t,"{5}"\t,"{6}"\t,"{7}"\t,"{8}"\t,{9},"{10}"\n'.format(
            point.result.place,
            point.starters,
            point.value,
            point.sum_value,
            point.result.person.first_name,
            point.result.person.last_name,
            '/'.join(str(c) for c in point.sum_categories),
            point.result.race.event.name,
            point.result.race.name,
            point.result.race.date,
            point.sum_notes))


type_map = {'text': TextOutput,
            'html': HtmlOutput,
            'json': JsonOutput,
            'csv': CsvOutput,
            }


def get_writer(output_format, *args, **kwargs):
    if output_format in type_map:
        return type_map[output_format](*args, **kwargs)
    else:
        raise NotImplemented()
