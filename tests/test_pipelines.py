
import responses
import os
import pandas as pd

# from ruffus_pipeline import swapi
from ruffus_pipeline import trenchrun as tr
import json


@responses.activate
def test_get_character_data():
    for i in range(1, 10):
        batch_url = "https://swapi.co/api/people/?page=%d&format=json" % i
        with open("tests/data/people_%s.json" % i, "r") as people_file:
            people_json = json.load(people_file)

        responses.add(
            responses.GET, batch_url, json=people_json, status=200,
        )

    output_file = os.path.join(
        os.path.dirname(__file__),
        "characters.csv"
    )

    tr.get_character_data(output_file)
    assert os.path.exists(output_file)

    df = pd.read_csv(output_file)
    assert df.shape[0] == 87
    assert set(list(df.columns)) == set([
        "name", "height", "species_id", "appearances"
    ])


def test_clean_data():

    input_file = os.path.join(
        os.path.dirname(__file__),
        "data",
        "characters.csv"
    )

    output_file = os.path.join(
        os.path.dirname(__file__),
        "cleaned.csv"
    )

    tr.clean_data(input_file, output_file)
    assert os.path.exists(output_file)

    df = pd.read_csv(output_file)
    # we take the top 10
    assert df.shape[0] == 10
    assert set(list(df.columns)) == set([
        "name", "height", "species_id", "appearances"
    ])
    # check sort
    df['prev_appearances'] = df["appearances"].shift(periods=1)
    df['sort_correct'] = df["appearances"] <= df["prev_appearances"]
    # by default the first row has to be True
    df.ix[0, "sort_correct"] = True
    assert df["sort_correct"].all()


@responses.activate
def test_get_species_data():

    for i in [1, 2, 3, 6]:
        species_url = "https://swapi.co/api/species/%s/?format=json" % i
        with open("tests/data/species_%s.json" % i, "r") as species_file:
            people_json = json.load(species_file)

        responses.add(
            responses.GET, species_url, json=people_json, status=200,
        )

    input_file = os.path.join(
        os.path.dirname(__file__),
        "data",
        "cleaned.csv"
    )

    output_file = os.path.join(
        os.path.dirname(__file__),
        "with_species.csv"
    )

    tr.get_species_data(input_file, output_file)
    assert os.path.exists(output_file)

    df = pd.read_csv(output_file)
    assert set(list(df.columns)) == set([
        "name", "height", "species", "appearances"
    ])


@responses.activate
def test_final_data():

    input_file = os.path.join(
        os.path.dirname(__file__),
        "data",
        "with_species.csv"
    )

    output_file = os.path.join(
        os.path.dirname(__file__),
        "final.csv"
    )

    tr.final_data(input_file, output_file)
    assert os.path.exists(output_file)

    df = pd.read_csv(output_file)
    assert set(list(df.columns)) == set([
        "name", "height", "species", "appearances"
    ])

    # check sort
    df['prev_height'] = df["height"].shift(periods=1)
    df['sort_correct'] = df["height"] <= df["prev_height"]
    # by default the first row has to be True
    df.ix[0, "sort_correct"] = True
    assert df["sort_correct"].all()
