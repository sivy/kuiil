import os
import requests

BASE_URL = "https://swapi.co/api"


def get_people():
    """
    This is cleaner and easier to test than the available swapi
    """
    url = os.path.join(BASE_URL, "people/?page=1&format=json")

    resp = requests.get(url, verify=False)
    data = resp.json()

    people_data = data['results'].copy()

    while data['next'] is not None:
        resp = requests.get(data["next"], verify=False)
        data = resp.json()
        people_data.extend(data["results"])

    return people_data


def get_species(species_id):
    url = os.path.join(BASE_URL, "species", str(species_id))
    url += "/?format=json"

    resp = requests.get(url, verify=False)
    data = resp.json()

    return data


def get_all_species():
    """
    This is cleaner and easier to test than the available swapi
    """
    url = os.path.join(BASE_URL, "species/?page=1&format=json")

    resp = requests.get(url, verify=False)
    data = resp.json()

    species_data = data['results'].copy()

    while data['next'] is not None:
        resp = requests.get(data["next"], verify=False)
        data = resp.json()
        species_data.extend(data["results"])

    return species_data
