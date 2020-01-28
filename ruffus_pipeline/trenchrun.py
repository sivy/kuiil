"""
A pipeline process for star wars data
"""
import logging
import os
import swapi

import pandas as pd
from ruffus import (
    cmdline, formatter,
    mkdir, originate, transform,
)

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger("trench")

DATADIR = os.environ.get("DATADIR", "data")

character_dir = os.path.join(DATADIR, "characters")
character_files = [os.path.join(character_dir, "characters.csv")]


@mkdir(character_dir)
@originate(character_files)
def get_character_data(output_file):
    people = swapi.get_all("people")

    def clean_species_val(url):
        return int(url.strip("/").split("/")[-1])

    data = []
    for character in people.items:
        print("%s: %s" % (character.name, character.species))
        if len(character.species) > 0:
            species_value = clean_species_val(character.species[0])
        else:
            species_value = None

        row = {
            "name": character.name,
            "height": character.height,
            "appearances": len(character.films),
            "species_id": species_value,
        }
        data.append(row)

    df = pd.DataFrame(data=data)
    df = df.set_index("appearances")
    df.to_csv(output_file)


clean_dir = os.path.join(DATADIR, "clean")
clean_files = [os.path.join(clean_dir, "clean.csv")]


@mkdir(clean_dir)
@transform(
    get_character_data,
    formatter(r".*?\.csv"),
    os.path.join(clean_dir, "cleaned.csv"))
def clean_data(input_file, output_file):

    df = pd.read_csv(input_file, index_col="appearances")
    # df = df.reset_index(drop=True)
    df = df.fillna("")

    remove_unknown_df = df[df['height'] != "unknown"].copy()

    df = remove_unknown_df.sort_index(ascending=False)
    df = df.head(10)
    df.to_csv(output_file)


species_dir = os.path.join(DATADIR, "with_species")
species_files = [os.path.join(species_dir, "with_species.csv")]


@mkdir(species_dir)
@transform(
    clean_data,
    formatter(r".*?\.csv"),
    os.path.join(species_dir, "with_species.csv"))
def get_species_data(input_file, output_file):
    df = pd.read_csv(input_file, index_col="appearances")
    # df = df.reset_index(drop=True)
    df = df.fillna("")

    def get_species(row):
        print(row)
        if row["species_id"] == "":
            row["species"] = "unknown"
            return row
        species_id = int(row["species_id"])
        species = swapi.get_species(species_id)
        # print(species)
        row["species"] = species.name if species.name else ""
        return row

    df = df.apply(get_species, axis=1)
    df = df.drop("species_id", axis=1)
    df.to_csv(output_file)


final_dir = os.path.join(DATADIR, "final")
final_files = [os.path.join(final_dir, "final.csv")]


@mkdir(final_dir)
@transform(
    get_species_data,
    formatter(r".*?\.csv"),
    os.path.join(final_dir, "final.csv"))
def final_data(input_file, output_file):
    df = pd.read_csv(input_file, index_col="appearances")
    df = df.reset_index()
    df = df.set_index("height")
    df = df.sort_index(ascending=False)
    df.to_csv(output_file)


def main():
    parser = cmdline.get_argparse(description="Trench Run pipeline")

    args = parser.parse_args()

    cmdline.run(args)


if __name__ == "__main__":
    main()
