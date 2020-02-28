# Kuuil Beam

Experimental implementation of the Kuiil test pipeline with the Apache Beam Python SDK.

## Getting Source Data

### Character Data

```
pipenv run python beam/kuill.py get_characters --output characters.csv
```

### Species Data

```
pipenv run python beam/kuill.py get_species --output species.csv
```

## Run the Pipeline

```
pipenv run python beam/kuill.py pipeline --characters characters.csv --species species.csv --output out.csv
```