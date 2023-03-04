"""
Create json files containing:
{QID: {PID: [List of QIDs]}}
filename: QID % NUM_FILES
"""
import os
import json
import argparse
from tqdm import tqdm
from typing import Dict, List
from os.path import exists, join
from collections import defaultdict
from fast_wikidata_db.indexing.utils import get_file_index


def jsonl_to_json(jsonl_dir: str):
    data_dict: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    with open(jsonl_dir, "r") as f:
        for line in f:
            data_dict = json.loads(line)
            for src_qcode, entity_rel in data_dict.items():
                for pcode, tar_qcode in entity_rel.items():
                    data_dict[src_qcode][pcode].extend(tar_qcode)
    with open(jsonl_dir.replace(".jsonl", ".json"), "w") as f:
        f.write(json.dumps(data_dict))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str, required=True, help="path to preprocessed dump of entity_rels")
    parser.add_argument("--output_dir", type=str, required=True, help="path to output directory")
    parser.add_argument("--num_files", type=int, default=10000)
    args = parser.parse_args()

    input_dir = join(args.input_dir, "entity_rels")

    output1_dir = join(args.output_dir, "entity_rels")
    if not exists(output1_dir):
        os.makedirs(output1_dir)

    output2_dir = join(args.output_dir, "entity_inv_rels")
    if not exists(output2_dir):
        os.makedirs(output2_dir)

    input_files = [join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".jsonl")]
    for input_file in tqdm(input_files):
        with open(input_file) as f:
            for line in f:
                data_dict = json.loads(line)
                src_qcode, pcode, tar_qcode = data_dict["qid"], data_dict["property_id"], data_dict["value"]

                src_dict = {src_qcode: {pcode: [tar_qcode]}}
                tar_dict = {tar_qcode: {pcode: [src_qcode]}}

                src_index = get_file_index(src_qcode, args.num_files)
                tar_index = get_file_index(tar_qcode, args.num_files)
                with open(join(output1_dir, str(src_index) + ".jsonl"), "a") as save_file:
                    save_file.write(f"{json.dumps(src_dict)}\n")
                with open(join(output2_dir, str(tar_index) + ".jsonl"), "a") as save_file:
                    save_file.write(f"{json.dumps(tar_dict)}\n")

    # Merge multiple dictionaries in jsonl into one dictionary
    # entity_rels
    for file_name in os.listdir(output1_dir):
        if file_name.endswith(".jsonl"):
            jsonl_to_json(join(output1_dir, file_name))
    # entity_inv_rels
    for file_name in os.listdir(output2_dir):
        if file_name.endswith(".jsonl"):
            jsonl_to_json(join(output2_dir, file_name))

    # Remove jsonl files
    for file_name in os.listdir(output1_dir):
        if file_name.endswith(".jsonl"):
            os.remove(join(output1_dir, file_name))
    for file_name in os.listdir(output2_dir):
        if file_name.endswith(".jsonl"):
            os.remove(join(output2_dir, file_name))


if __name__ == "__main__":
    main()