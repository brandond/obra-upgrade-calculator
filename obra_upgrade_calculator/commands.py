#!/usr/bin/env python
import logging
from datetime import datetime

import click

from .outputs import type_map


@click.command()
@click.option('--type', type=click.Choice(['cyclocross']), required=True)
@click.option('--format', type=click.Choice(sorted(type_map.keys())), default='text')
@click.option('--scrape/--no-scrape', default=True)
@click.option('--strict/--no-strict', default=False)
@click.option('--debug/--no-debug', default=False)
def cli(type, format, scrape, strict, debug):
    log_level = 'DEBUG' if debug else 'INFO'
    logging.basicConfig(level=log_level)

    # Import these after setting up logging otherwise we don't get logs
    from .scrapers import scrape_year, scrape_new, scrape_recent
    from .upgrades import recalculate_points, print_points, sum_points

    if scrape:
        # Scrape last two years of results
        year = datetime.now().year
        scrape_year(year - 1, type)
        scrape_year(year, type)

        # Load in anything new
        scrape_new()

        # Check for updates to anything touched in the last three days
        scrape_recent(3)

        # Calculate points from new data
        recalculate_points(type)
        sum_points(type, strict)

    # Finally, output data
    print_points(type, format)


if __name__ == '__main__':
    cli()
