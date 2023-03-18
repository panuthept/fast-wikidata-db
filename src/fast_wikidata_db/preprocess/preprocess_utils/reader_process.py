import gzip
import itertools
from tqdm import tqdm
from pathlib import Path
from multiprocessing import Queue, Value


def count_lines(input_file: Path, max_lines_to_read: int):
    cnt = 0
    with gzip.open(input_file, 'rb') as f:
        for _ in tqdm(f, desc="Counting lines"):
            cnt += 1
            if max_lines_to_read > 0 and cnt >= max_lines_to_read:
                break
    return cnt


def read_data(input_file: Path, num_lines_read: Value, max_lines_to_read: int, work_queue: Queue):
    """
    Reads the data from the input file and pushes it to the output queue.
    :param input_file: Path to the input file.
    :param num_lines_read: Value to store the number of lines in the input file.
    :param max_lines_to_read: Maximum number of lines to read from the input file (for testing).
    :param work_queue: Queue to push the data to.
    """
    with gzip.GzipFile(input_file, "r") as f:
        num_lines = 0
        for ln in tqdm(f, total=max_lines_to_read, position=0, desc="Reading wikidata dump"):
            if ln == b"[\n" or ln == b"]\n":
                continue
            if ln.endswith(b",\n"):  # all but the last element
                obj = ln[:-2]
            else:
                obj = ln
            num_lines += 1
            work_queue.put(obj)
            if 0 < max_lines_to_read <= num_lines:
                break
    num_lines_read.value = num_lines
    return


def parallel_read_data(input_file: Path, num_lines_read: Value, start_line: int, end_line: int, work_queue: Queue, position: int = 0):
    progress_bar = tqdm(total=end_line - start_line, position=position, desc=f"Reading wikidata dump: {position}")
    with gzip.GzipFile(input_file, "r") as f:
        for num_lines, ln in enumerate(itertools.islice(f, start_line, end_line)):
            if ln == b"[\n" or ln == b"]\n":
                continue

            if ln.endswith(b",\n"):  # all but the last element
                obj = ln[:-2]
            else:
                obj = ln

            work_queue.put(obj)
            num_lines_read.value += 1
            progress_bar.update(1)

            if 0 < end_line - start_line <= num_lines:
                break
    return