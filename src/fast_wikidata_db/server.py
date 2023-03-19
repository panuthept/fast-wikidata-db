import uvicorn
from fastapi import FastAPI
from fast_wikidata_db.wikidata import Wikidata


app = FastAPI()
wikidata = Wikidata()


@app.get("/check_existance")
async def check_existance(qcode: str):
    return wikidata.is_exists(qcode)


@app.get("/retrieve/title")
async def retrieve_entity_title(qcode: str):
    return wikidata.retrieve_entity_title(qcode)


@app.get("/retrieve/aliases")
async def retrieve_entity_aliases(qcode: str):
    return wikidata.retrieve_entity_aliases(qcode)


@app.get("/retrieve/description")
async def retrieve_entity_description(qcode: str):
    return wikidata.retrieve_entity_description(qcode)


@app.get("/retrieve/relations")
async def retrieve_entity_relations(qcode: str, inverse_relation: bool = False):
    return wikidata.retrieve_entity_relations(qcode, inverse_relation=inverse_relation)


@app.get("/retrieve/entities")
async def retrieve_entities(qcode: str, pcode: str, inverse_relation: bool = False):
    return wikidata.retrieve_entities(qcode, pcode, inverse_relation=inverse_relation)


@app.get("/retrieve/values")
async def retrieve_values(qcode: str, pcode: str):
    return wikidata.retrieve_values(qcode, pcode)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000)