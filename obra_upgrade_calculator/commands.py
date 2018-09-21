#!/usr/bin/env python
import logging
from datetime import datetime

import click


@click.command()
@click.option('--type', type=click.Choice(['cyclocross']), required=True)
@click.option('--debug/--no-debug', default=False)
def cli(type, debug):
    log_level = 'DEBUG' if debug else 'INFO'
    logging.basicConfig(level=log_level)

    # Import these after setting up logging otherwise we don't get logs
    from .scrapers import scrape_year, scrape_new, scrape_recent
    from .upgrades import recalculate_points, print_points

    # Scrape last two years of results
    year = datetime.now().year
    scrape_year(year - 1, type)
    scrape_year(year, type)

    # Load in anything new
    scrape_new()

    # Check for updates to anything touched in the last three days
    scrape_recent(3)

    # Calculate and print points
    recalculate_points(type)
    print_points(type)


if __name__ == '__main__':
    cli()
