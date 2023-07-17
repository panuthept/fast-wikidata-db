# fast-wikidata-db
A python library provides the easiest way to store and retrieve data from Wikidata.

## Installation
```bash
pip install .
```

## Usage
```python
from fast_wikidata_db.wikidata import Wikidata

# Create a Wikidata object (the nessary files will be downloaded automatically)
wikidata = Wikidata()

wikidata.is_exists("Q8")
# >> True
wikidata.retrieve_entity_title("Q8")
# >> 'happiness'
wikidata.retrieve_entity_aliases("Q8")
# >> ['happiness', 'joy', 'happy', 'Happiness']
wikidata.retrieve_entity_description("Q8")
# >> 'mental or emotional state of well-being characterized by pleasant emotions'
wikidata.retrieve_entity_relations("Q8", inverse_relation=False)
# >> {'P31': ['Q331769', 'Q60539479', 'Q9415'], 'P279': ['Q16748867'], ...}
wikidata.retrieve_entity_relations("Q8", inverse_relation=True)
# >> {'P279': ['Q625143', 'Q11613371', 'Q97061894'], ...}
wikidata.retrieve_entities("Q8", "P31")
# >> ['Q331769', 'Q60539479', 'Q9415']
```