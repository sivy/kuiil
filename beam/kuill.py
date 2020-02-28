
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
    Parses raw character data into a
    dictionary of the data

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
        so that the data can be combined with the species data
        by species_id
        """
        try:
            row = list(csv.reader([elem]))[0]
            logging.debug("Got character row: %s", row)
            appearances, name, height, species_id = row
            appearances = int(appearances)
            height = int(height) if height != "unknown" else -1.0
            species_id = int(float(species_id)) if species_id else 0

            yield {
                "appearances": appearances,
                "name": name,
                "height": height,
                "species_id": species_id,
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
            logging.debug("Got species row: %s", row)
            yield {
                "species_id": int(row[0]) if row[0] else 0,
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
    """
    Main entry point; defines and runs the kuill pipeline.
    """
    parser = argparse.ArgumentParser()

    sp = parser.add_subparsers()
    #
    get_characters_parser = sp.add_parser("get_characters")
    get_characters_parser.add_argument("--output", default="characters.csv")
    get_characters_parser.set_defaults(command=get_characters_command)

    get_species_parser = sp.add_parser("get_species")
    get_species_parser.add_argument("--output", default="species.csv")
    get_species_parser.set_defaults(command=get_species_command)

    pipeline_parser = sp.add_parser("pipeline")

    pipeline_parser.add_argument(
        '--characters', type=str,
        required=True,
        help='Path to an input file.')

    pipeline_parser.add_argument(
        '--species', type=str,
        required=True,
        help='Path to an input file.')

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

    def join_data(k, v):
        logging.debug("join_data: %s", (k, v))
        import itertools
        return itertools.product(v['characters'], v['species'])

    def merge_data(row):
        logging.debug("merge_data: %s", row)
        character_data, species_data = row
        character_data.update(species_data)
        del character_data['species_id']
        return character_data

    def by_all_appearances(row):
        logging.debug("by_appearances: %s", row)
        return (row["appearances"], row)

    def appearance_key_for_element(element):
        """
        element is a dictionary of character data
        return the value of the "appearances" key
        """
        return element["appearances"]

    def height_key_for_element(element):
        """
        element is a dictionary of character data
        return the value of the "appearances" key
        """
        return element["height"]

    def format_csv(data):
        lines = ["{height},{appearances},{name},{species}".format(
            **row) for row in data]
        return "\n".join(lines)

    def resplit_data(data):
        """
        data is a list with one element, a list of rows?
        """
        logging.debug("resplit_data: %s", data)
        for row in data:
            yield row

    with beam.Pipeline(options=options) as p:
        char_inputs = (
            p
            | 'ReadCharInputText' >> beam.io.ReadFromText(
                args.characters, skip_header_lines=1)
        )

        spec_inputs = (
            p
            | 'ReadSpecInputText' >> beam.io.ReadFromText(
                args.species, skip_header_lines=1)
        )

        characters = (
            char_inputs
            | "parse chars" >> beam.ParDo(ParseCharacterFn())
            | "key_char" >> beam.Map(
                lambda c: (c["species_id"], c))
        )

        species = (
            spec_inputs
            | beam.ParDo(ParseSpeciesFn())
            | "key_spec" >> beam.Map(
                lambda s: (s["species_id"], s))
        )

        joined = (
            {"characters": characters,
                "species": species}
            | beam.CoGroupByKey()
            | beam.FlatMapTuple(join_data)
        )

        merged = (
            joined
            | beam.Map(merge_data)
        )

        top = (
            merged
            | "top by appearances" >> beam.combiners.Top.Of(
                10, key=appearance_key_for_element)
            | "re-split for height" >> beam.FlatMap(resplit_data)
            | "top by height" >> beam.combiners.Top.Of(
                10, key=height_key_for_element)
        )

        output = (  # noqa
            top
            | "format_csv" >> beam.Map(format_csv)
            | 'WriteCharacterData' >> beam.io.WriteToText(args.output)
        )


if __name__ == "__main__":
    run()
