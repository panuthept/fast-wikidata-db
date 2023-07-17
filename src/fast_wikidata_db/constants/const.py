import os


DEFAULT_DATA_DIR = os.path.join(os.path.expanduser("~"), ".cache", "fast_wikidata_db", "data")
DEFAULT_DB_DIR = DEFAULT_DATA_DIR + "/wikidata_db"

S3_BUCKET = "fast-wikidata-db"
S3_KEYS = [
    "labels.lmdb",
    "aliases.lmdb",
    "descriptions.lmdb",
    "entity_values.lmdb",
    "entity_rels.lmdb",
    "entity_inv_rels.lmdb",
]
DATA_URLS = {
    "labels.lmdb": "https://fast-wikidata-db.s3.ap-southeast-1.amazonaws.com/labels.lmdb",
    "aliases.lmdb": "https://fast-wikidata-db.s3.ap-southeast-1.amazonaws.com/aliases.lmdb",
    "descriptions.lmdb": "https://fast-wikidata-db.s3.ap-southeast-1.amazonaws.com/descriptions.lmdb",
    "entity_values.lmdb": "https://fast-wikidata-db.s3.ap-southeast-1.amazonaws.com/entity_values.lmdb",
    "entity_rels.lmdb": "https://fast-wikidata-db.s3.ap-southeast-1.amazonaws.com/entity_rels.lmdb",
    "entity_inv_rels.lmdb": "https://fast-wikidata-db.s3.ap-southeast-1.amazonaws.com/entity_inv_rels.lmdb",
}