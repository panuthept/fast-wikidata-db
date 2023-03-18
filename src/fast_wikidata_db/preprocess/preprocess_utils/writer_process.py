import ujson
import shutil
from tqdm import tqdm
from pathlib import Path
from multiprocessing import Queue
from typing import Dict, Any, List


TABLE_NAMES = [
    'labels', 'descriptions', 'aliases', 'entity_values', 'wikipedia_links', 'entity_rels', 'entity_inv_rels'
]


class Table:
    def __init__(self, path: Path, batch_nums: int, table_name: str, remove_existing: bool = False):
        self.table_name = table_name
        self.table_dir = path / table_name
        self.batch_nums = batch_nums

        if self.table_dir.exists() and remove_existing:
            shutil.rmtree(self.table_dir)
        self.table_dir.mkdir(parents=True, exist_ok=True)

    def write(self, json_value: List[Dict[str, Any]]):
        for json_obj in json_value:
            file_index = int(json_obj['qid'][1:]) % self.batch_nums
            with open(self.table_dir / f"{file_index:d}.jsonl", 'a') as f:
                f.write(ujson.dumps(json_obj, ensure_ascii=False) + '\n')


class Writer:
    def __init__(self, path: Path, batch_nums: int, verbose: bool, remove_existing: bool = False):
        self.verbose = verbose
        self.output_tables = {table_name: Table(path, batch_nums, table_name, remove_existing) for table_name in TABLE_NAMES}
        if self.verbose:
            self.progress_bar = tqdm(position=10, desc=f"Writing processed dump ")

    def write(self, json_object: Dict[str, Any]):
        for key, value in json_object.items():
            if len(value) > 0:
                self.output_tables[key].write(value)
                if self.verbose and key == "entity_rels":
                    self.progress_bar.update(10)


def write_data(path: Path, batch_nums: int, verbose: bool, outout_queue: Queue, remove_existing: bool = False):
    writer = Writer(path, batch_nums, verbose, remove_existing)
    while True:
        json_object = outout_queue.get()
        if json_object is None:
            break
        writer.write(json_object)
