#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from datetime import date

import click

from .data import DISCIPLINE_MAP
from .outputs import OUTPUT_MAP


@click.command()
@click.option('--discipline', type=click.Choice(DISCIPLINE_MAP.keys()), required=True)
@click.option('--output', type=click.Choice(sorted(OUTPUT_MAP.keys())), default='text')
@click.option('--scrape/--no-scrape', default=True)
@click.option('--debug/--no-debug', default=False)
def cli(discipline, output, scrape, debug):
    log_level = 'DEBUG' if debug else 'INFO'
    logging.basicConfig(level=log_level, format='%(levelname)s:%(module)s.%(funcName)s:%(message)s')

    # Import these after setting up logging otherwise we don't get logs
    from .scrapers import clean_events, scrape_year, scrape_new, scrape_parents, scrape_recent
    from .upgrades import confirm_pending_upgrades, recalculate_points, print_points, sum_points
    from .rankings import calculate_race_ranks
    from .models import db

    with db.atomic('IMMEDIATE'):
        if scrape:
            # Scrape last 5 years of results
            cur_year = date.today().year
            for year in range(cur_year - 6, cur_year + 1):
                scrape_year(year, discipline)
                scrape_parents(year, discipline)
                clean_events(year, discipline)

            # Load in anything new
            scrape_new(discipline)

            # Check for updates to anything touched in the last three days
            scrape_recent(discipline, 3)

        # Calculate points from new data
        if recalculate_points(discipline, incremental=False):
            calculate_race_ranks(discipline, incremental=False)
            sum_points(discipline)
            confirm_pending_upgrades(discipline)

    # Finally, output data
    print_points(discipline, output)


if __name__ == '__main__':
    cli()
