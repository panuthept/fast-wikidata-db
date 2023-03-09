""" 
Wikidata Dump Processor
This script preprocesses the raw Wikidata dump (in JSON format) and sorts triples into 8 "tables": labels, descriptions, aliases, entity_rels, external_ids, entity_values, qualifiers, and wikipedia_links. See the README for more information on each table.
"""

import os
import time
import argparse
import multiprocessing
from pathlib import Path
from multiprocessing import Queue, Process
from fast_wikidata_db.constants.const import DEFAULT_DATA_DIR
from fast_wikidata_db.preprocess.preprocess_utils.writer_process import write_data
from fast_wikidata_db.preprocess.preprocess_utils.worker_process import process_data
from fast_wikidata_db.preprocess.preprocess_utils.reader_process import count_lines, read_data


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dir', type=str, required=True, help='path to gz wikidata json dump')
    parser.add_argument('--output_dir', type=str, default=None, help='path to output directory')
    parser.add_argument('--language_id', type=str, default='en', help='language identifier')
    parser.add_argument('--processes', type=int, default=90, help="number of concurrent processes to spin off. ")
    parser.add_argument('--batch_nums', type=int, default=10000)
    parser.add_argument('--num_lines_read', type=int, default=-1,
                        help='Terminate after num_lines_read lines are read. Useful for debugging.')
    parser.add_argument('--num_lines_in_dump', type=int, default=-1, help='Number of lines in dump. If -1, we will count the number of lines.')
    return parser


def main():
    start = time.time()
    args = get_arg_parser().parse_args()
    print(f"ARGS: {args}")

    output_dir = args.output_dir
    if os.path.exists(output_dir):
        output_dir = os.path.join(DEFAULT_DATA_DIR, args.language_id)
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    input_dir = Path(args.input_dir)
    assert input_dir.exists(), f"Input file {input_dir} does not exist"


    max_lines_to_read = args.num_lines_read
    if args.num_lines_in_dump <= 0:
        total_num_lines = count_lines(input_dir, max_lines_to_read)
    else:
        total_num_lines = args.num_lines_in_dump
    max_lines_to_read = total_num_lines
    print(f"Total number of lines: {total_num_lines}")

    print("Starting processes")
    maxsize = 10 * args.processes

    # Queues for inputs/outputs
    output_queue = Queue(maxsize=maxsize)
    work_queue = Queue(maxsize=maxsize)

    # Processes for reading/processing/writing
    num_lines_read = multiprocessing.Value("i", 0)
    read_process = Process(
        target=read_data,
        args=(input_dir, num_lines_read, max_lines_to_read, work_queue)
    )

    read_process.start()

    write_process = Process(
        target=write_data,
        args=(output_dir, args.batch_nums, total_num_lines, output_queue)
    )
    write_process.start()

    work_processes = []
    for _ in range(max(1, args.processes-2)):
        work_process = Process(
            target=process_data,
            args=(args.language_id, work_queue, output_queue)
        )
        work_process.daemon = True
        work_process.start()
        work_processes.append(work_process)

    read_process.join()
    print(f"Done! Read {num_lines_read.value} lines")
    # Cause all worker process to quit
    for work_process in work_processes:
        work_queue.put(None)
    # Now join the work processes
    for work_process in work_processes:
        work_process.join()
    output_queue.put(None)
    write_process.join()

    print(f"Finished processing {num_lines_read.value} in {time.time() - start}s")


if __name__ == "__main__":
    main()
