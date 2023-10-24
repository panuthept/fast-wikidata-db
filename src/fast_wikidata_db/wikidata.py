import os
from typing import List, Dict
from fast_wikidata_db.s3_download import db_download
from fast_wikidata_db.constants.const import DEFAULT_DB_DIR
from fast_wikidata_db.indexing.lmdb_wrapper import LmdbImmutableDict


class Wikidata:
    def __init__(self, database_dir: str = None):
        self.database_dir = database_dir if database_dir is not None else DEFAULT_DB_DIR

        # Download the database if it does not exist
        if not os.path.exists(self.database_dir):
            os.makedirs(self.database_dir)
        if not os.path.exists(os.path.join(self.database_dir, "labels.lmdb")):
            db_download(self.database_dir, s3_key="labels.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "aliases.lmdb")):
            db_download(self.database_dir, s3_key="aliases.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "descriptions.lmdb")):
            db_download(self.database_dir, s3_key="descriptions.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "entity_values.lmdb")):
            db_download(self.database_dir, s3_key="entity_values.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "entity_rels.lmdb")):
            db_download(self.database_dir, s3_key="entity_rels.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "entity_inv_rels.lmdb")):
            db_download(self.database_dir, s3_key="entity_inv_rels.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "wikidata_qid_to_wikipedia_title.lmdb")):
            db_download(self.database_dir, s3_key="wikidata_qid_to_wikipedia_title.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "wikipedia_title_to_wikidata_qid.lmdb")):
            db_download(self.database_dir, s3_key="wikipedia_title_to_wikidata_qid.lmdb")
        if not os.path.exists(os.path.join(self.database_dir, "redirects.lmdb")):
            db_download(self.database_dir, s3_key="redirects.lmdb")

        self.labels = LmdbImmutableDict(os.path.join(self.database_dir, "labels.lmdb"))
        self.aliases = LmdbImmutableDict(os.path.join(self.database_dir, "aliases.lmdb"))
        self.descriptions = LmdbImmutableDict(os.path.join(self.database_dir, "descriptions.lmdb"))
        self.entity_values = LmdbImmutableDict(os.path.join(self.database_dir, "entity_values.lmdb"))
        self.entity_rels = LmdbImmutableDict(os.path.join(self.database_dir, "entity_rels.lmdb"))
        self.entity_inv_rels = LmdbImmutableDict(os.path.join(self.database_dir, "entity_inv_rels.lmdb"))
        self.wikidata_qid_to_wikipedia_title = LmdbImmutableDict(os.path.join(self.database_dir, "wikidata_qid_to_wikipedia_title.lmdb"))
        self.wikipedia_title_to_wikidata_qid = LmdbImmutableDict(os.path.join(self.database_dir, "wikipedia_title_to_wikidata_qid.lmdb"))
        self.redirects = LmdbImmutableDict(os.path.join(self.database_dir, "redirects.lmdb"))
        self.wiki_to_qcode = LmdbImmutableDict(os.path.join(self.database_dir, "wiki_to_qcode.lmdb"))

    def redirect_wikipedia_title(self, wikipedia_title: str) -> str:
        wikipedia_title = wikipedia_title.replace(" ", "_").replace("&lt;", "<").replace("&gt;", ">").replace("&le;", "≤").replace("&ge;", "≥")
        if wikipedia_title == "":
            return " "
        wikipedia_title = wikipedia_title[0].upper() + wikipedia_title[1:]
        if wikipedia_title in self.redirects:
            wikipedia_title = self.redirects[wikipedia_title]
        wikipedia_title = wikipedia_title.replace("_", " ")
        return wikipedia_title

    def wikipedia_to_wikidata(self, wikipedia_title: str) -> str:
        wikipedia_title = self.redirect_wikipedia_title(wikipedia_title)
        qcode = self.wikipedia_title_to_wikidata_qid.get(wikipedia_title, None)
        if qcode is None:
            wikipedia_title = wikipedia_title.replace(" ", "_")
            qcode = self.wiki_to_qcode.get(wikipedia_title, None)
        return qcode

    def retrieve_wikipedia_title(self, qcode: str) -> str:
        wikipedia_title = self.wikidata_qid_to_wikipedia_title.get(qcode, None)
        return wikipedia_title

    def retrieve_entity_title(self, qcode: str) -> str:
        return self.labels.get(qcode, None)

    def retrieve_entity_aliases(self, qcode: str) -> List[str]:
        return self.aliases.get(qcode, None)

    def retrieve_entity_description(self, qcode: str) -> str:
        return self.descriptions.get(qcode, None)

    def retrieve_entity_relations(self, qcode: str, inverse_relation: bool = False) -> Dict[str, List[str]]:
        if inverse_relation:
            return self.entity_inv_rels.get(qcode, dict())
        else:
            rels = self.entity_rels.get(qcode, dict())
            rels.update(self.entity_values.get(qcode, dict()))
            return rels

    def retrieve_entities(self, qcode: str, pcode: str, inverse_relation: bool = False) -> List[str]:
        entity_qcodes = []
        rels = self.retrieve_entity_relations(qcode, inverse_relation=inverse_relation)
        if len(rels) > 0:
            entity_qcodes = rels.get(pcode, list())
        return entity_qcodes
    
    def retrieve_values(self, qcode: str, pcode: str) -> List[str]:
        entity_values = []
        rels = self.entity_values.get(qcode, dict())
        if len(rels) > 0:
            entity_values = rels.get(pcode, list())
        return entity_values
    
    def is_exists(self, qcode: str) -> bool:
        return self.retrieve_entity_title(qcode) is not None
    
    def is_disambiguation_qcodes(self, qcode: str) -> bool:
        desc = self.retrieve_entity_description(qcode)
        if desc is not None:
            return desc == "Wikimedia disambiguation page"
        return False
    
    def is_description_available(self, qcode: str) -> bool:
        desc = self.retrieve_entity_description(qcode)
        if desc is not None:
            return desc != "no description"
        return False


if __name__ == "__main__":
    from tqdm import trange

    wikidata = Wikidata()
    qcode = wikidata.wikipedia_to_wikidata("Radio Nicaragua")
    print(qcode)
    wikipedia_title = wikidata.wikidata_to_wikipedia("Q2937389")
    print(wikipedia_title)
    # qcode = wikidata.wikipedia_to_wikidata("Capital Cities/ABC")
    # print(qcode)
    # qcode = wikidata.wikipedia_to_wikidata("Capital Cities/ABC Inc.")
    # print(qcode)

    # qcode = "Q2937389"
    # wikipedia_title = wikidata.wikidata_to_wikipedia(qcode)
    # print(wikipedia_title)
    # wikipedia_title = wikidata.redirect_wikipedia_title(wikipedia_title)
    # print(wikipedia_title)

    # print("Testing Retrieval Speed...")
    # for i in trange(10000000):
    #     qcode = f"Q{i}"
    #     if wikidata.is_exists(qcode):
    #         title = wikidata.retrieve_entity_title(qcode)
    #         aliases = wikidata.retrieve_entity_aliases(qcode)
    #         decs = wikidata.retrieve_entity_description(qcode)
    #         wikipedia_title = wikidata.wikidata_to_wikipedia(qcode)