import os
import ujson
from tqdm import tqdm
from collections import defaultdict
from fast_wikidata_db.indexing.lmdb_wrapper import LmdbImmutableDict


def index_labels(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    lmdb_dict = LmdbImmutableDict(output_dir + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing labels", position=0):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = {}
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']] = data['label']
        for key, value in merged_data.items():
            lmdb_dict.put(key=key, value=value)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict.write_to_compacted_file()


def index_descriptions(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    lmdb_dict = LmdbImmutableDict(output_dir + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing descriptions", position=1):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = {}
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']] = data['description']
        for key, value in merged_data.items():
            lmdb_dict.put(key=key, value=value)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict.write_to_compacted_file()


def index_wikipedia_links(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    output_dir_1 = os.path.join(base_dir, "wikidata_qid_to_wikipedia_title")
    output_dir_2 = os.path.join(base_dir, "wikipedia_title_to_wikidata_qid")
    lmdb_dict_1 = LmdbImmutableDict(output_dir_1 + ".lmdb", write_mode=True)
    lmdb_dict_2 = LmdbImmutableDict(output_dir_2 + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing wikipedia_links", position=2):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = {}
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']] = data['wiki_title']
        for qid, title in merged_data.items():
            lmdb_dict_1.put(key=qid, value=title)
            lmdb_dict_2.put(key=title, value=qid)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict_1.write_to_compacted_file()
    lmdb_dict_2.write_to_compacted_file()


def index_aliases(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    lmdb_dict = LmdbImmutableDict(output_dir + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing aliases", position=3):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = defaultdict(list)
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']].append(data['alias'])
        for key, value in merged_data.items():
            lmdb_dict.put(key=key, value=value)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict.write_to_compacted_file()


def index_entity_values(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    lmdb_dict = LmdbImmutableDict(output_dir + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing entity_values", position=4):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = defaultdict(lambda: defaultdict(list))
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']][data['property_id']].append(data['value'])
        for key, value in merged_data.items():
            lmdb_dict.put(key=key, value=value)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict.write_to_compacted_file()


def index_entity_rels(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    lmdb_dict = LmdbImmutableDict(output_dir + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing entity_rels", position=5):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = defaultdict(lambda: defaultdict(list))
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']][data['property_id']].append(data['value'])
        for key, value in merged_data.items():
            lmdb_dict.put(key=key, value=value)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict.write_to_compacted_file()


def index_entity_inv_rels(input_dir, output_dir, remove_old=False):
    base_dir = os.path.dirname(output_dir)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    lmdb_dict = LmdbImmutableDict(output_dir + ".lmdb", write_mode=True)
    filenames = [filename for filename in os.listdir(input_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing entity_inv_rels", position=6):
        data_dir = os.path.join(input_dir, filename)
        # Read
        merged_data = defaultdict(lambda: defaultdict(list))
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']][data['property_id']].append(data['value'])
        for key, value in merged_data.items():
            lmdb_dict.put(key=key, value=value)
        # Write
        # save_dir = os.path.join(output_dir, filename.replace(".jsonl", ".json"))
        # with open(save_dir, 'w') as f:
        #     f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)
    lmdb_dict.write_to_compacted_file()