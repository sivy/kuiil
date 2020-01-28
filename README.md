# kuiil

> I have spoken

## Requirements

Using the Star Wars API (https://swapi.co/)

1. [x] Find the ten characters who appear in the most Star Wars films
2. [x] Sort those ten characters by height in descending order (i.e., tallest first)
3. [x] Produce a CSV with the following columns: name, species, height, appearances
4. [ ] Send the CSV to httpbin.org
5. [ ] Create automated tests that validate your code

## Notes

I've implemented my pipeline ("trenchrun") with a library called [ruffus](http://www.ruffus.org.uk/) which I'm fond of. Ruffus is useful because it's based around filesets that can be inspected between stages, and a pipeline will pick up from a failed stage based on what previous artifacts exist (so, not unlike Make in that way).

I considered writing my own SW API client but when I saw they had a python module already I used that because who's got time?

### Stages implemented in this pipeline:

- Get Character Data
    - uses the `swapi` python module to gat all the chracters from the "people" API
    - outputs csv with `name`, `height`, `appearances`, `species_id`
- Clean Data
    - reads the csv from the previous stage
    - removes any rows with "unknown" `height`
    - sorts by `appearance` (descending)
    - takes the top 10
    - outputs csv with `name`, `height`, `appearances`, `species_id`
- Get Species Data
    - reads the csv from the previous stage
    - uses the `swapi` API to get the species data for each row
    - adds the `species` column
    - drops the `species_id` column
    - outputs csv with `name`, `height`, `appearances`, `species`
- Final Data
    - reads the csv from the previous stage
    - sorts data by `height` (descending)
    - outputs csv with `name`, `height`, `appearances`, `species`


Ruffus is probably a bit of overkill, but I've used it for quite a few utility pipelines and it works well. :D

