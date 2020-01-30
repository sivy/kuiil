# kuiil

> I have spoken

## Requirements

Using the Star Wars API (https://swapi.co/)

1. [x] Find the ten characters who appear in the most Star Wars films
2. [x] Sort those ten characters by height in descending order (i.e., tallest first)
3. [x] Produce a CSV with the following columns: name, species, height, appearances
4. [x] Send the CSV to httpbin.org
5. [x] Create automated tests that validate your code

## Notes

I've implemented my pipeline ("trenchrun") with a library called [ruffus](http://www.ruffus.org.uk/) which I'm fond of. Ruffus is useful because it's based around filesets that can be inspected between stages, and a pipeline will pick up from a failed stage based on what previous artifacts exist (so, not unlike Make in that way). I also use `pandas` for manipulating tabular data.

I started writing with the Star Wars API client found at [a python module already](https://github.com/phalt/swapi-python) but it has some inefficiencies that result in extraneous calls, and also makes testing the code more complicated, so I ended up simplifying to a couple functions.

Ruffus is probably a bit of overkill, but I've used it for quite a few utility ETL pipelines and it works well. :D

### Stages implemented in this pipeline:

- Get Character Data (stage `get_character_data`)
    - uses the `swapi` python module to gat all the chracters from the "people" API
    - outputs csv with `name`, `height`, `appearances`, `species_id`
- Clean Data (stage `clean_data`)
    - reads the csv from the previous stage
    - removes any rows with "unknown" `height`
    - sorts by `appearance` (descending)
    - takes the top 10
    - outputs csv with `name`, `height`, `appearances`, `species_id`
- Get Species Data  (stage `get_species_data`)
    - reads the csv from the previous stage
    - uses the `swapi` API to get the species data for each row
    - adds the `species` column
    - drops the `species_id` column
    - outputs csv with `name`, `height`, `appearances`, `species`
- Final Data (stage `final_data`)
    - reads the csv from the previous stage
    - sorts data by `height` (descending)
    - outputs csv with `name`, `height`, `appearances`, `species`
- Publish Data (stage `publish_data`)
    - reads the csv from the previous stage
    - posts the data to <https://httpbin.org/post>
    - parses the `json` data as then write it to `receipt.yaml` demonstrating the published API call.

### Running the Code

I use `pipenv` for most of my python projects, but this code also has a requirements.txt for using `pip`.

```
# make a virtualenv first if you choose
% pip install -r requirements.txt
```

#### Run Tests

```
EXPORT DATADIR=/path/to/data
PYTHONPATH=. pytest -q tests/test_pipelines.py
```

(Will throw some urllib3 warnings I did not bother to silence)

#### Run Pipeline

```
PYTHONPATH=. python pipeline/trenchrun.py
```

To rerun a particular stage, delete the output file `data/<stage>/*.csv`, and run `python pipeline/trenchrun.py -T <stage>` (see stage names above).

### Reviewing Data

Open `$DATADIR` to find the following directories and files:

- `characters/`
    - `characters.csv`
- `clean/`
    - `cleaned.csv`
- `with_species/`
    - `with_species.csv`
- `final_data/`
    - `final_data.csv`
- `published/`
    - `receipt.yaml`

Each of these can be inspected to observce the results of the transformations applied in the stage.
