import shutil
from multiprocessing import Queue
from pathlib import Path
from typing import Dict, Any, List
import time
import ujson

TABLE_NAMES = [
    'labels', 'descriptions', 'aliases', 'external_ids', 'entity_values', 'qualifiers', 'wikipedia_links', 'entity_rels', 'entity_inv_rels'
]


class Table:
    def __init__(self, path: Path, batch_nums: int, table_name: str):
        self.table_name = table_name
        self.table_dir = path / table_name
        self.batch_nums = batch_nums

        if self.table_dir.exists():
            shutil.rmtree(self.table_dir)
        self.table_dir.mkdir(parents=True, exist_ok=False)

    def write(self, json_value: List[Dict[str, Any]]):
        for json_obj in json_value:
            if self.table_name == 'qualifiers':
                file_index = hash(json_obj['claim_id']) % self.batch_nums
            else:
                file_index = int(json_obj['qid'][1:]) % self.batch_nums
            with open(self.table_dir / f"{file_index:d}.jsonl", 'a') as f:
                f.write(ujson.dumps(json_obj, ensure_ascii=False) + '\n')


class Writer:
    def __init__(self, path: Path, batch_nums: int, total_num_lines: int):
        self.cur_num_lines = 0
        self.total_num_lines = total_num_lines
        self.start_time = time.time()
        self.output_tables = {table_name: Table(path, batch_nums, table_name) for table_name in TABLE_NAMES}

    def write(self, json_object: Dict[str, Any]):
        self.cur_num_lines += 1
        for key, value in json_object.items():
            if len(value) > 0:
                self.output_tables[key].write(value)
        if self.cur_num_lines % 200000 == 0:
            time_elapsed = time.time() - self.start_time
            estimated_time = time_elapsed * (self.total_num_lines - self.cur_num_lines) / (200000*3600)
            print(f"{self.cur_num_lines}/{self.total_num_lines} lines written in {time_elapsed:.2f}s. "
                  f"Estimated time to completion is {estimated_time:.2f} hours.")
            self.start_time = time.time()


def write_data(path: Path, batch_nums: int, total_num_lines: int, outout_queue: Queue):
    writer = Writer(path, batch_nums, total_num_lines)
    while True:
        json_object = outout_queue.get()
        if json_object is None:
            break
        writer.write(json_object)
