import os
import ujson
from tqdm import tqdm
from collections import defaultdict


def index_labels(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing labels", position=0):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = {}
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']] = data['label']
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)


def index_descriptions(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing descriptions", position=1):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = {}
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']] = data['description']
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)


def index_wikipedia_links(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing wikipedia_titles", position=2):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = {}
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']] = data['wiki_title']
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)


def index_aliases(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing aliases", position=3):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = defaultdict(list)
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']].append(data['alias'])
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)


def index_entity_values(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing entity_values", position=4):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = defaultdict(lambda: defaultdict(list))
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']][data['property_id']].append(data['value'])
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)


def index_entity_rels(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing entity_rels", position=5):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = defaultdict(lambda: defaultdict(list))
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']][data['property_id']].append(data['value'])
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)


def index_entity_inv_rels(data_dir, remove_old=True):
    filenames = [filename for filename in os.listdir(data_dir) if filename.endswith(".jsonl")]
    for filename in tqdm(filenames, desc="Indexing entity_inv_rels", position=6):
        data_dir = os.path.join(data_dir, filename)
        # Read
        merged_data = defaultdict(lambda: defaultdict(list))
        with open(data_dir, 'r') as f:
            for line in f:
                data = ujson.loads(line)
                merged_data[data['qid']][data['property_id']].append(data['value'])
        # Write
        with open(data_dir, 'w') as f:
            f.write(ujson.dumps(merged_data, ensure_ascii=False))
        # Remove
        if remove_old:
            os.remove(data_dir)