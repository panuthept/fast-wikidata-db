# fast-wikidata-db
A python library provides the easiest way to store and retrieve data from Wikidata.

## Installation
```bash
pip install .
```

## Usage
```python
from fast_wikidata_db.wikidata import Wikidata

wikidata = Wikidata() # Create a Wikidata object (the nessary files will be downloaded automatically)

wikidata.is_exists("Q8") # Check if entity Q8 exists
wikidata.retrieve_entity_title("Q8") # Retrieve the title of entity Q8
wikidata.retrieve_entity_aliases("Q8") # Retrieve the aliases of entity Q8
wikidata.retrieve_entity_description("Q8") # Retrieve the description of entity Q8
wikidata.retrieve_entity_relations("Q8", inverse_relation=False) # Retrieve the relations of entity Q8
wikidata.retrieve_entities("Q8", "P31") # Retrieve the entities of entity Q8 with relation P31