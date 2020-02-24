
import argparse
import csv
import logging
from pipeline.trenchrun import get_character_data, get_all_species_data

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

logging.getLogger("apache_beam").setLevel(logging.ERROR)


class ParseCharacterFn(beam.DoFn):
    """
    Parses raw character data into a tuple of
    species_i and python dictionary of the data

    appearances,name,height,species_id
    """

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(
            self.__class__, "num_parse_errors"
        )

    def process(self, elem):
        """
        Returns a key, val tuple of (species_id, {character data})
        so that the data can b compbined with the species data
        by species_id
        """
        try:
            row = list(csv.reader([elem]))[0]
            logging.debug("Got row: %s", row)

            yield {
                "appearances": row[0],
                "name": row[1],
                "height": float(row[2]) if row[2] != "unknown" else -1.0,
                "species_id": row[3],
            }
        except Exception as e:
            self.num_parse_errors.inc()
            logging.error(
                "Parse error %s on '%s'", str(e), elem)


class ParseSpeciesFn(beam.DoFn):
    """
    Parses raw character data into a python dictionary

    appearances,name,height,species_id
    """

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(
            self.__class__, "num_parse_errors"
        )

    def process(self, elem):
        """
        """
        try:
            row = list(csv.reader([elem]))[0]
            logging.debug("Got row: %s", row)
            yield {
                "species_id": row[0],
                "species": row[1],
            }
        except Exception as e:
            self.num_parse_errors.inc()
            logging.error(
                "Parse error %s on '%s'", str(e), elem)


class CharacterData(beam.PTransform):
    def expand(sel, pcoll):
        return (
            pcoll
            | 'ParseCharacterFn' >> beam.ParDo(ParseCharacterFn())
        )


class SpeciesData(beam.PTransform):
    def expand(sel, pcoll):
        return (
            pcoll
            | 'ParseSpeciesFn' >> beam.ParDo(ParseSpeciesFn())
        )


def get_characters_command(args):
    get_character_data(args.output)


def get_species_command(args):
    get_all_species_data(args.output)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the kuill pipeline."""
    parser = argparse.ArgumentParser()

    sp = parser.add_subparsers()
    get_characters_parser = sp.add_parser("get_data")
    get_characters_parser.add_argument("--output", default="characters.csv")
    get_characters_parser.set_defaults(command=get_characters_command)

    get_species_parser = sp.add_parser("get_species")
    get_species_parser.add_argument("--output", default="species.csv")
    get_species_parser.set_defaults(command=get_species_command)

    pipeline_parser = sp.add_parser("pipeline")

    pipeline_parser.add_argument(
        '--input', type=str,
        required=True,
        help='Path to the input file.')

    pipeline_parser.add_argument(
        '--output', type=str,
        required=True,
        help='Path to the output file(s).')

    pipeline_parser.set_defaults(command=None)

    args, pipeline_args = parser.parse_known_args(argv)

    if args.command:
        # handle get_data_command
        args.command(args)
        return

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_session

    def join_lists(k, v):
        import itertools
        itertools.product(v['characters'], v['species'])

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadInputText' >> beam.io.ReadFromText(
                args.input, skip_header_lines=1)
        )

        characters = (
            p
            | 'CharacterData' >> CharacterData()
            | "key_char" >> beam.Map(
                lambda c: (c["species_id"], c))
        )
        # logging.debug(characters)

        species = (
            p
            | 'SpeciesData' >> SpeciesData()
            | "key_spec" >> beam.Map(
                lambda s: (s["species_id"], s))
        )
        logging.debug(species)

        joined = {"characters": characters,
                  "species": species} | beam.CoGroupByKey()
        # | beam.FlatMap(join_lists)
        # | 'WriteCharacterData' >> beam.io.WriteToText(args.output)

        logging.debug(joined)

        # (
        #     p
        #     # | 'ReadInputText' >> beam.io.ReadFromText(
        #     #     args.input, skip_header_lines=1)
        #     # | 'CharacterData' >> CharacterData()
        #     # | "key_char" >> beam.Map(
        #     #     lambda c: (c["species_id"], c))
        #     # | 'SpeciesData' >> SpeciesData()
        #     # | "key_spec" >> beam.Map(
        #     #     lambda s: (s["species_id"], s))
        #     | 'WriteCharacterData' >> beam.io.WriteToText(args.output)
        # )


if __name__ == "__main__":
    run()
