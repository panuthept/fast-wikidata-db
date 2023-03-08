import shutil
from tqdm import tqdm
from multiprocessing import Queue
from pathlib import Path
from typing import Dict, Any, List
import ujson

TABLE_NAMES = [
    'labels', 'descriptions', 'aliases', 'entity_values', 'wikipedia_links', 'entity_rels', 'entity_inv_rels'
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
            file_index = int(json_obj['qid'][1:]) % self.batch_nums
            with open(self.table_dir / f"{file_index:d}.jsonl", 'a') as f:
                f.write(ujson.dumps(json_obj, ensure_ascii=False) + '\n')


class Writer:
    def __init__(self, path: Path, batch_nums: int, total_num_lines: int):
        self.output_tables = {table_name: Table(path, batch_nums, table_name) for table_name in TABLE_NAMES}
        self.progress_bars = {table_name: tqdm(total=total_num_lines, position=i + 1, desc=f"Writing {table_name}") for i, table_name in enumerate(TABLE_NAMES)}

    def write(self, json_object: Dict[str, Any]):
        for key, value in json_object.items():
            if len(value) > 0:
                self.output_tables[key].write(value)
                self.progress_bars[key].update(1)


def write_data(path: Path, batch_nums: int, total_num_lines: int, outout_queue: Queue):
    writer = Writer(path, batch_nums, total_num_lines)
    while True:
        json_object = outout_queue.get()
        if json_object is None:
            break
        writer.write(json_object)
