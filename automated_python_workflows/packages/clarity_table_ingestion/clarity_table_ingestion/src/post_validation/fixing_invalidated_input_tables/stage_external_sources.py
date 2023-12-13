import os
import logging

from ingestion_utilities.dbt_functions import execute_dbt_command

from clarity_table_ingestion.src.utilities.common_functions import clear_folder_contents
from clarity_table_ingestion.src.utilities.common_functions import LANDING_TABLES, BATCH_ID
from clarity_table_ingestion.src.config import PROJECT_OUTPUT_FOLDER_FIXING_INVALIDATED_TABLES

logger = logging.getLogger(os.path.basename(__file__))

EXTERNAL_SOURCES_DBT_OUTPUT_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER_FIXING_INVALIDATED_TABLES, 'dbt-stage-external-sources')

def _write_stage_external_sources_results(write_folder_path: str, batch_id: str, captured_text: str) -> None:

    file_name = f'stg-ext-src-{batch_id}.txt'
    full_file_path = os.path.join(write_folder_path, file_name)
    with open(full_file_path, "w") as file:
        file.write(captured_text)

def _get_select_args() -> (str, str):

    table_args = ' '.join([f'raw_clarity_aah.RAW_{table}' for table in LANDING_TABLES])
    select_args = f'select: {table_args}'
    select_args_for_user_in_dbt = f'"select: {table_args}"'

    return (select_args, select_args_for_user_in_dbt)

def generate_command_to_stage_all_external_sources() -> str:

    command_start = r'run-operation stage_external_sources --args'
    select_args, select_args_for_user_in_dbt = _get_select_args()

    full_command_str = f"{command_start} {select_args_for_user_in_dbt}"
    full_command_list = command_start.split(" ") + [select_args]
    logger.debug(f"full_command_list: {full_command_list}")
    logger.info(f"full_command_str to stage all external sources - run this in dbt for debugging: {full_command_str}")

    return full_command_str, full_command_list

def stage_external_sources() -> dict:
    
    clear_folder_contents(EXTERNAL_SOURCES_DBT_OUTPUT_FOLDER)
    logger.info(f"about to execute stage-external-sources macro for the following tables: {LANDING_TABLES}")
    _, command_list  = generate_command_to_stage_all_external_sources()

    captured_text, response = execute_dbt_command(command_list)
    results = {
        "success": response.success,
        "exception": response.exception
    }

    _write_stage_external_sources_results(EXTERNAL_SOURCES_DBT_OUTPUT_FOLDER, BATCH_ID, captured_text)

    return results

if __name__ == "__main__":
    generate_command_to_stage_all_external_sources()
    # stage_external_sources()