""" 
Wikidata Dump Indexing
"""

import os
import argparse
from pathlib import Path
from multiprocessing import Process
from fast_wikidata_db.constants.const import DEFAULT_DATA_DIR
from fast_wikidata_db.preprocess.preprocess_utils.writer_process import TABLE_NAMES
from fast_wikidata_db.indexing.indexing_utils import index_labels, index_descriptions, index_aliases, index_entity_values, index_wikipedia_links, index_entity_rels, index_entity_inv_rels


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dir', type=str, required=True, help='path to preprocessed wikidata dump')
    parser.add_argument('--output_dir', type=str, default=DEFAULT_DATA_DIR, help='path to output directory')
    parser.add_argument('--remove_old', default=False, action='store_true', help='whether to remove old preprocessed files')
    return parser


def main():
    args = get_arg_parser().parse_args()
    print(f"ARGS: {args}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    input_dir = Path(args.input_dir)
    assert input_dir.exists(), f"Input file {input_dir} does not exist"

    indexer = {
        'labels': index_labels,
        'descriptions': index_descriptions,
        'aliases': index_aliases,
        'entity_values': index_entity_values,
        'wikipedia_links': index_wikipedia_links,
        'entity_rels': index_entity_rels,
        'entity_inv_rels': index_entity_inv_rels,
    }

    processes = []
    for table_name in TABLE_NAMES:
        process = Process(
            target=indexer[table_name],
            args=(os.path.join(input_dir, table_name), os.path.join(output_dir, table_name), args.remove_old)
        )
        process.start()
        processes.append(process)

    # Wait for all processes to finish
    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
