import os
from typing import List, Dict
from dataclasses import dataclass
from fast_wikidata_db.wikidata.cache import LRUCache
from fast_wikidata_db.constants.const import DEFAULT_DATA_DIR
from fast_wikidata_db.indexing.lmdb_wrapper import LmdbImmutableDict


@dataclass
class Entity:
    title: str
    desc: str
    qcode: str
    aliases: List[str]
    pcodes: List[str]
    inv_pcodes: List[str]


class Wikidata:
    def __init__(self, database_dir: str = None, cache_size: int = 1000000):
        self.database_dir = database_dir if database_dir is not None else DEFAULT_DATA_DIR + "/wikidata_db"
        self.cache = LRUCache(cache_size)

        self.labels = LmdbImmutableDict(os.path.join(self.database_dir, "labels.lmdb"))
        self.aliases = LmdbImmutableDict(os.path.join(self.database_dir, "aliases.lmdb"))
        self.descriptions = LmdbImmutableDict(os.path.join(self.database_dir, "descriptions.lmdb"))
        self.entity_rels = LmdbImmutableDict(os.path.join(self.database_dir, "entity_rels.lmdb"))
        self.entity_inv_rels = LmdbImmutableDict(os.path.join(self.database_dir, "entity_inv_rels.lmdb"))

    def get_title(self, qcode: str) -> str:
        pass

    def get_desc(self, qcode: str) -> str:
        pass

    def get_aliases(self, qcode: str) -> List[str]:
        pass

    def get_pcodes(self, qcode: str) -> List[str]:
        pass

    def get_inv_pcodes(self, qcode: str) -> List[str]:
        pass

    def get_entity_rels(self, qcode: str) -> Dict[str, List[str]]:
        pass

    def get_entity_inv_rels(self, qcode: str) -> Dict[str, List[str]]:
        pass

    def get_entities(self, qcode: str, pcode: str = None, inv_pcode: str = None) -> List[str]:
        assert pcode is not None or inv_pcode is not None, "Either pcode or inv_pcode must be specified"
        if pcode is not None:
            entity_rel = self.get_entity_rels(qcode)
            if entity_rel is not None:
                tar_qcodes = entity_rel.get(pcode, None)
        else:
            entity_rel = self.get_entity_inv_rels(qcode)
            if entity_rel is not None:
                tar_qcodes = entity_rel.get(inv_pcode, None)

        if tar_qcodes is not None:
            tar_entities = []
            for tar_qcode in tar_qcodes:
                title = self.get_title(tar_qcode)
                desc = self.get_desc(tar_qcode)
                aliases = self.get_aliases(tar_qcode)
                pcodes = self.get_pcodes(tar_qcode)
                inv_pcodes = self.get_inv_pcodes(tar_qcode)
                tar_entity = Entity(
                    title=title,
                    desc=desc,
                    qcode=tar_qcode,
                    aliases=aliases,
                    pcodes=pcodes,
                    inv_pcodes=inv_pcodes,
                )
                tar_entities.append(tar_entity)
            return tar_entities