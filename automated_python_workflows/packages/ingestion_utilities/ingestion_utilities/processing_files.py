import os
import csv
from typing import Tuple, List, Iterable, Sized

def get_files_from_folder(folder_path: str) -> list[str]:
    file_list = os.listdir(folder_path)
    return file_list

def get_file_name_and_extension(file_name: str) -> Tuple[str, str]:
    name, extension = file_name.rsplit(".", 1)
    return name, extension

def check_length(input: Iterable[Sized], max_value: int) -> None:
    if len(input) > max_value:
        raise ValueError(f"the input {input} exceeds the max value set of {max_value}")

def get_input_items_from_csv(file_path: str) -> List[str]:
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        raw_tables = []
        for row in reader:
            raw_tables.extend(row)
    return raw_tables

def check_file_extension(extension: str, expected_extension_type: str) -> None:
    if extension != expected_extension_type:
        raise ValueError(f"Invalid file extension. Expected: {expected_extension_type}, Got: {extension}")

def process_single_csv_input_from_folder(folder_path: str) -> Tuple[str, List[str]]:
    """Outputs: (file_name, input_items_upper_case)"""
    file_list = get_files_from_folder(folder_path)
    check_length(input=file_list, max_value=1)
    full_file_name = file_list[0]
    file_name, extension = get_file_name_and_extension(full_file_name)
    check_file_extension(extension=extension, expected_extension_type="csv")
    full_input_file_path = os.path.join(folder_path, full_file_name)
    input_items = get_input_items_from_csv(full_input_file_path)
    input_items_upper = [item.upper() for item in input_items]
    return file_name, input_items_upper

if __name__ == "__main__":
    pass