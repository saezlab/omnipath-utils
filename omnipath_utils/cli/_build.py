"""Build command for omnipath-utils database."""

from __future__ import annotations

import argparse
import logging


def build_cmd(args: list[str]):
    """Execute the build command."""
    parser = argparse.ArgumentParser(description='Build omnipath-utils database')
    parser.add_argument('--db-url', default=None, help='PostgreSQL connection URL')
    parser.add_argument('--organisms', nargs='+', type=int, default=[9606],
                        help='NCBI Taxonomy IDs to build for (default: 9606)')
    parser.add_argument('--ref-only', action='store_true',
                        help='Only build reference tables (id_types, organisms, backends)')
    parser.add_argument('-v', '--verbose', action='store_true')

    opts = parser.parse_args(args)

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    from omnipath_utils.db._build import DatabaseBuilder

    builder = DatabaseBuilder(db_url=opts.db_url)

    if opts.ref_only:
        builder.build_reference_tables()
    else:
        builder.build_all(organisms=opts.organisms)

    print('Build complete!')
