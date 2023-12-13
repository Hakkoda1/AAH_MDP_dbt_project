import os
import sys
import pandas as pd
import yaml
import logging
import csv
from typing import List, Tuple

from snowflake.connector.connection import SnowflakeConnection

from ingestion_utilities.connections import connect_to_snowflake
from ingestion_utilities.common_functions import concat_list_elements_for_snowflake_query, execute_snowflake_query
from ingestion_utilities.processing_files import process_single_csv_input_from_folder

from clarity_view_ingestion.src.config import PROJECT_INPUT_FOLDER, SRC_VIEW_SCRIPTS_OUTPUT_PATH

INPUT_VIEWS_FOLDER_PATH = os.path.join(PROJECT_INPUT_FOLDER, 'views_to_ingest')

logger = logging.getLogger(os.path.basename(__file__))

"""
script takes in a list of views (currently hard coded) and gets the sql for the views from snowflake.  The output is a csv file
"""


def _build_query(views) -> str:
    view_names: str = concat_list_elements_for_snowflake_query(views)
    query = f'''
    SELECT TABLE_NAME, SOURCE_VIEW_SCRIPT
    FROM MDP_RAW_DEV.GENERAL_MGMT.SOURCE_TABLE_CONFIG
    WHERE IS_VIEW = TRUE
    AND TABLE_NAME IN ({view_names})
    AND SOURCE_KEY = 1;
    '''
    return query

def _check_data(df: pd.DataFrame, views: list) -> None:
    if (len(df) != len(views)):
        print(f"there is a mismatch in the number of views requested and the number of views returned. {len(df)} views returned and {len(views)} views requested")
        print(f"views returned: {df.index.tolist()}")
        print(f"views requested: {views}")

def _write_src_view_script_dataframe_to_yaml(df, file_path) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as yaml_file:
        for index, row in df.iterrows():
            view = index
            query = row['SOURCE_VIEW_SCRIPT']
            # Write view and query to YAML file
            yaml.dump({view: query}, yaml_file, default_flow_style=False, allow_unicode=True)
    print(f"DataFrame successfully written to {file_path}")

def get_all_src_view_scripts() -> None:
    """for a list of views grab the source code used to build each view from snowflake and write the data to a yaml file"""
    file_name, views = process_single_csv_input_from_folder(INPUT_VIEWS_FOLDER_PATH)
    snowflake_context_session = connect_to_snowflake()
    query = _build_query(views)
    df = execute_snowflake_query(snowflake_context_session, query)
    df = df.set_index("TABLE_NAME")
    _check_data(df, views)
    _write_src_view_script_dataframe_to_yaml(df, SRC_VIEW_SCRIPTS_OUTPUT_PATH)

if __name__ == "__main__":
    get_all_src_view_scripts()