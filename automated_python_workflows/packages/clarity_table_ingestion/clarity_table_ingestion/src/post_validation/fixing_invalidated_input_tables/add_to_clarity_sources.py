import yaml
import re
import os
import logging

from ingestion_utilities.common_functions import get_unique_tables_in_clarity_sources_manual
from ingestion_utilities.dbt_functions import execute_dbt_command

from ingestion_utilities.global_vars import CLARITY_SOURCES_MANUAL_PATH, CLARITY_SOURCES_PATH

from clarity_table_ingestion.src.utilities.common_functions import clear_folder_contents

from clarity_table_ingestion.src.config import PROJECT_OUTPUT_FOLDER_FIXING_INVALIDATED_TABLES
from clarity_table_ingestion.src.utilities.common_functions import INPUT_TABLES, VALIDATION_DF

logger = logging.getLogger(os.path.basename(__file__))

YML_GEN_SELECTOR_OUTPUT_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER_FIXING_INVALIDATED_TABLES, "yml_gen_selector")

def _get_tables_in_clarity_sources(clarity_sources_manual_path, clarity_sources_path) -> set:

    def get_unique_tables_in_clarity_sources(clarity_sources_path) -> list:
    
        with open(clarity_sources_path, 'r') as file:
            clarity_sources_manual = yaml.safe_load(file)

        unique_tables = []
        for table in clarity_sources_manual['sources'][0]['tables']:
            if table['name'][-7:] == "_DELETE": #_DELETE tables are duplicates
                continue
            unique_tables.append(table['name'])
        logger.debug(f"there are {len(unique_tables)} unique tables in clarity sources")

        return unique_tables

    clarity_sources_manual_tables = get_unique_tables_in_clarity_sources_manual(clarity_sources_manual_path)
    clarity_sources_tables = get_unique_tables_in_clarity_sources(clarity_sources_path)

    return set(clarity_sources_manual_tables + clarity_sources_tables)

def _get_new_table_args(missing_tables) -> list:

    new_table_args_format = "{'table_name': 'TABLE_NAME' ,'type': 'LOAD_TYPE'}"
    new_table_args_list = []
    for table_name in missing_tables:

        load_type = VALIDATION_DF.loc[table_name, "Load_type"].lower()

        new_table_args = new_table_args_format.replace('TABLE_NAME', table_name)
        new_table_args = new_table_args.replace('LOAD_TYPE', load_type)

        new_table_args_list.append(new_table_args)

    logger.debug(f"new table args list: {new_table_args_list}")

    return new_table_args_list

def _write_clarity_sources_yaml_for_new_tables(yml_gen_selector_output_folder, captured_txt) -> str:

    new_table_clarity_sources_yaml_file_path = os.path.join(yml_gen_selector_output_folder, f"new_table_clarity_sources.yaml")

    regex_expression = r"tables:(.*)"
    match = re.search(regex_expression, captured_txt, re.DOTALL)

    # Ensure the directory exists or create it
    os.makedirs(yml_gen_selector_output_folder, exist_ok=True)

    if match:
        yaml_section = match.group(1).strip()

    else:
        raise Exception(f"regex expression: {regex_expression} did not match captured text: {captured_txt}")

    with open(new_table_clarity_sources_yaml_file_path, 'w') as file:
        file.write(yaml_section)

    return new_table_clarity_sources_yaml_file_path

def _append_new_yaml_to_clarity_sources(clarity_sources_path, new_table_yaml_file_path) -> None:
    with open(new_table_yaml_file_path, "r") as source_file:
        new_yaml = source_file.read()
    
    with open(clarity_sources_path, "a") as destination_file:
        destination_file.write('\n  ')
        destination_file.write(new_yaml)

    return None       

def get_missing_tables_in_clarity_sources() -> list:

    clarity_sources_tables = _get_tables_in_clarity_sources(CLARITY_SOURCES_MANUAL_PATH, CLARITY_SOURCES_PATH)
    missing_tables = []
    for table in INPUT_TABLES:
        if f"RAW_{table}" not in clarity_sources_tables:
            logger.debug(f"{table} is not in clarity_sources anywhere!")
            missing_tables.append(table)

    logger.info(f"missing tables: {missing_tables}")

    return missing_tables
    
def generate_yml_gen_selector_command(missing_tables) -> list:
    command_start = "run-operation yml_gen_selector --args"
    args = r"{include_list: [LIST_OF_NEW_TABLE_ARGS] }"

    new_table_args_list = _get_new_table_args(missing_tables)
    new_table_args_str = ",".join(new_table_args_list)
    args : str = args.replace("LIST_OF_NEW_TABLE_ARGS", new_table_args_str)  

    full_command_str = f"{command_start} {args}"
    full_command_list = command_start.split(" ") + [args]
    logger.debug(f"full_command_list: {full_command_list}")
    logger.info(f"full_command_str for yml gen selector command: {full_command_str}")

    return full_command_list

def add_missing_tables_to_clarity_sources() -> None:
    """
    New tables that we want to ingest and bring into snowflake via dbt must be added to the clarity sources yaml file.
    If you try to build tables that are not included in this yml file, dbt will through an error.
    Tables can be in two different yml files: clarity_sources.yml and clarity_sources_manual.yml

    This function ...
        1. gets the tables we want to ingest
        2. figures out which tables are not in clarity sources
        3. generates the command to generate the yaml for the missing tables via existing macro
        4. executes the macro and captures the results
        5. adds the results from this macro to your clarity_sources.yml file
    """

    clear_folder_contents(YML_GEN_SELECTOR_OUTPUT_FOLDER)
    logger.debug("about to adding missing tables to clarity sources")

    missing_tables: list = get_missing_tables_in_clarity_sources()

    if len(missing_tables) == 0:
        logger.info(f"there were no missing tables to add to clarity sources!")
        return None

    command_list = generate_yml_gen_selector_command(missing_tables)
    captured_txt, _ = execute_dbt_command(command_list)
    new_table_clarity_sources_yaml_file_path = _write_clarity_sources_yaml_for_new_tables(YML_GEN_SELECTOR_OUTPUT_FOLDER, captured_txt)
    _append_new_yaml_to_clarity_sources(CLARITY_SOURCES_PATH, new_table_clarity_sources_yaml_file_path)
    
def __main__():
    missing_tables = get_missing_tables_in_clarity_sources()
    generate_yml_gen_selector_command(missing_tables)