import os
import re
import json
import yaml
import logging
import csv

from typing import Dict, Tuple, List

logger = logging.getLogger(os.path.basename(__file__))

SRC_VIEW_SCRIPTS_OUTPUT_PATH = r'C:\Users\C88831\Projects\MDP_DBT\MDP_DBT_Cloud\python_workflows\clarity_file_ingestion_project\clarity_file_ingestion\src\views\outputs\source_veiw_scripts.yaml'

def read_yaml_to_dict(file_path) -> None:
    with open(file_path, 'r', encoding='utf-8') as yaml_file:
        data_dict = yaml.safe_load(yaml_file)
    return data_dict

def write_dict_to_json(dictionary, file_path):
    """
    Writes a dictionary to a JSON file.

    Parameters:
    - dictionary (dict): The dictionary to be written to the JSON file.
    - file_path (str): The path to the JSON file.

    Returns:
    - None
    """
    with open(file_path, 'w') as json_file:
        json.dump(dictionary, json_file, indent=2)

def read_json_to_dict(file_path):
    """
    Reads a JSON file and returns its content as a dictionary.

    Parameters:
    - file_path (str): The path to the JSON file.

    Returns:
    - dict: The dictionary loaded from the JSON file.
    """
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    return data
