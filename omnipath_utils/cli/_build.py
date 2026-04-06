"""Build command for omnipath-utils database."""

from __future__ import annotations

import logging
import argparse


def build_cmd(args: list[str]):
    """Execute the build command."""
    parser = argparse.ArgumentParser(
        description="Build omnipath-utils database"
    )
    parser.add_argument(
        "--db-url", default=None, help="PostgreSQL connection URL"
    )
    parser.add_argument(
        "--organisms",
        nargs="+",
        type=int,
        default=[9606],
        help="NCBI Taxonomy IDs to build for (default: 9606)",
    )
    parser.add_argument(
        "--ref-only",
        action="store_true",
        help="Only build reference tables (id_types, organisms, backends)",
    )
    parser.add_argument(
        "--ftp",
        action="store_true",
        help="Build from full UniProt FTP idmapping.dat.gz (all organisms, ~18GB)",
    )
    parser.add_argument(
        "--preset",
        choices=["minimal", "standard", "model", "full"],
        default=None,
        help="Build preset",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="Directory for Parquet file exports",
    )
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="List available presets and exit",
    )
    parser.add_argument("-v", "--verbose", action="store_true")

    opts = parser.parse_args(args)

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if opts.list_presets:
        from omnipath_utils.db._presets import PRESETS

        for name, config in PRESETS.items():
            print(f"{name}: {config["description"]}")
            print(
                f"  Organisms: {config["organisms"]}"
            )
            print(
                f"  Metabolites: {config["metabolite"]},"
                f" miRNA: {config["mirna"]},"
                f" Orthology: {config["orthology"]}"
            )
        return

    from omnipath_utils.db._build import DatabaseBuilder

    builder = DatabaseBuilder(db_url=opts.db_url)

    if opts.preset:
        builder.build_preset(opts.preset, parquet_dir=opts.parquet_dir)
    elif opts.ftp:
        builder.build_reference_tables()
        builder.populate_from_ftp()
    elif opts.ref_only:
        builder.build_reference_tables()
    else:
        builder.build_all(organisms=opts.organisms)

    print("Build complete!")
