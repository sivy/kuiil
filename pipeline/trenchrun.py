"""
A pipeline process for star wars data
"""
import logging
import os
from pipeline import swapi
import yaml
import requests

import pandas as pd
from ruffus import (
    cmdline, formatter, pipeline_run,
    mkdir, originate, transform,
)

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger("trench")

DATADIR = os.environ.get("DATADIR", "data")

character_dir = os.path.join(DATADIR, "characters")
character_files = [os.path.join(character_dir, "characters.csv")]


def get_all_species_data(output_file):
    """
    Get the character data from the Star Wars `species` API

    {
        "name": "Hutt",
        "classification": "gastropod",
        "designation": "sentient",
        "average_height": "300",
        "skin_colors": "green, brown, tan",
        "hair_colors": "n/a",
        "eye_colors": "yellow, red",
        "average_lifespan": "1000",
        "homeworld": "https://swapi.co/api/planets/24/",
        "language": "Huttese",
        "people": [
            "https://swapi.co/api/people/16/"
        ],
        "films": [
            "https://swapi.co/api/films/3/",
            "https://swapi.co/api/films/1/"
        ],
        "created": "2014-12-10T17:12:50.410000Z",
        "edited": "2014-12-20T21:36:42.146000Z",
        "url": "https://swapi.co/api/species/5/"
    }
    """
    species = swapi.get_all_species()

    def clean_species_val(url):
        return int(url.strip("/").split("/")[-1])

    data = []
    for s in species:
        row = {
            "name": s["name"],
            "id": s["url"].strip("/").split("/")[-1],
        }
        data.append(row)

    df = pd.DataFrame(data=data)
    df = df.set_index("id")

    df.to_csv(output_file)


@mkdir(character_dir)
@originate(character_files)
def get_character_data(output_file):
    """
    Get the character data from the Star Wars `people` API
    """
    people = swapi.get_people()

    def clean_species_val(url):
        return int(url.strip("/").split("/")[-1])

    data = []
    for character in people:
        print("%s: %s" % (character["name"], character["species"]))
        if len(character["species"]) > 0:
            species_value = clean_species_val(character["species"][0])
        else:
            species_value = None

        row = {
            "name": character["name"],
            "height": character["height"],
            "appearances": len(character["films"]),
            "species_id": species_value,
        }
        data.append(row)

    df = pd.DataFrame(data=data)
    df = df.set_index("appearance")
    df.to_csv(output_file)


clean_dir = os.path.join(DATADIR, "clean")
clean_files = [os.path.join(clean_dir, "clean.csv")]


@mkdir(clean_dir)
@transform(
    get_character_data,
    formatter(r".*?\.csv"),
    os.path.join(clean_dir, "cleaned.csv"))
def clean_data(input_file, output_file):
    """
    Remove character rows with "unknown" height.
    Take the top ten characters, sorted by appearances, descending)
    """
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
    """
    Gets the species names from the Star Wars `species` API
    """
    df = pd.read_csv(input_file, index_col="appearances")
    # df = df.reset_index(drop=True)
    df = df.fillna("")

    def get_species(row):
        """
        API returns a url for species, which we've already reduced to
        the unique ID. Use the Star Wars API to get the species name
        """
        if row["species_id"] == "":
            row["species"] = "unknown"
            return row
        species_id = int(row["species_id"])
        species = swapi.get_species(species_id)
        # print(species)
        row["species"] = species["name"] if species["name"] else ""
        return row

    df = df.apply(get_species, axis=1)
    df = df.drop("species_id", axis=1)
    df.to_csv(output_file)


final_dir = os.path.join(DATADIR, "final_data")
final_files = [os.path.join(final_dir, "final_data.csv")]


@mkdir(final_dir)
@transform(
    get_species_data,
    formatter(r".*?\.csv"),
    os.path.join(final_dir, "final_data.csv"))
def final_data(input_file, output_file):
    """
    Order by height, descending
    """
    df = pd.read_csv(input_file, index_col="appearances")
    df = df.reset_index()
    df = df.set_index("height")
    df = df.sort_index(ascending=False)
    df.to_csv(output_file)


publish_dir = os.path.join(DATADIR, "published")


@mkdir(publish_dir)
@transform(
    final_data,
    formatter(r".*?\.csv"),
    os.path.join(publish_dir, "receipt.yaml"))
def publish_data(input_file, output_file):

    with open(input_file, "r") as data_file:
        data = data_file.read()

    resp = requests.post("https://httpbin.org/post", data=data, verify=False)

    resp_data = resp.json()
    with open(output_file, "w") as receipt_file:
        yaml.dump(resp_data, receipt_file)


def main():
    parser = cmdline.get_argparse(description="Trench Run pipeline")

    args = parser.parse_args()

    if args.target_tasks:
        cmdline.run(args)

    else:
        pipeline_run(publish_data)


if __name__ == "__main__":
    main()
