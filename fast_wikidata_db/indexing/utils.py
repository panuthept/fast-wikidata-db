def get_file_index(qcode: str, num_files: int):
    return int(qcode[1:]) % num_files