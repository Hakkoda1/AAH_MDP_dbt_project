import os
import yaml
import re
import logging

from ingestion_utilities.dbt_functions import execute_dbt_command

from clarity_table_ingestion.src.config import PROJECT_OUTPUT_FOLDER
from clarity_table_ingestion.src.utilities.common_functions import VALIDATED_TABLES


CURRENT_DIRECTORY = os.getcwd()
REFINED_VIEWS_PATH = os.path.join(CURRENT_DIRECTORY, r"models\refined\clarity_aah\views\_refined_views.yml")
YML_MOD_OUTPUT_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, "refined_views_yml")
NEW_TABLE_REFINED_VIEWS_YAML_FILE_PATH = os.path.join(YML_MOD_OUTPUT_FOLDER, "new_table_refined_views.yaml")
RAW_TXT_FROM_DBT_CMD = os.path.join(YML_MOD_OUTPUT_FOLDER, "raw_txt_from_dbt_cmd.txt")


logger = logging.getLogger(os.path.basename(__file__))

def get_missing_tables_in_refined_views() -> list[str]:

    def get_models_in_refined_views() -> set[str]:
        with open(REFINED_VIEWS_PATH, 'r', encoding='utf8') as file:
            refined_views = yaml.safe_load(file)
        tables = set()
        for table in refined_views['models']:
            tables.add(table['name'])

        return tables

    refined_views_models: set = get_models_in_refined_views()
    missing_tables = []

    for table_name in VALIDATED_TABLES:
        model_name = re.sub(r'^zc_', 'lkp_clr_', table_name.lower())
        if model_name not in refined_views_models:
            missing_tables.append(table_name)

    return missing_tables

def _generate_yml_gen_mod_command_list(missing_tables: list[str]) -> list[str]:
    command_list = "run-operation yml_gen_mod --args".split(" ")

    full_args = r"{tables_location_sf: 'MDP_REFINED_DEV.CLARITY_AAH', include_list: LIST_OF_TABLES}"
    missing_tables = [table.replace("ZC_", "LKP_CLR_") for table in missing_tables]
    full_args: str = full_args.replace("LIST_OF_TABLES", str(missing_tables))

    command_list.append(full_args)
    logger.debug(f"command_list: {command_list}")

    return command_list

def generate_yml_gen_mod_command_str() -> str:

    if len(VALIDATED_TABLES) == 0:
        logger.warn("There are no validated tables to work with!")
        return None
    
    missing_tables: list = get_missing_tables_in_refined_views()

    if len(missing_tables) == 0:
        return "There are no missing tables to add to refined_views.yml"

    command_list: list[str] = _generate_yml_gen_mod_command_list(missing_tables)

    dbt_cloud_cmd = " ".join(command_list)
    logger.info(f"generate full yml_gen_mod_command command: {dbt_cloud_cmd}")

    return dbt_cloud_cmd

def write_refined_views_yaml_for_new_tables(raw_txt: str) -> None:

    def write_temp_yaml_file(yaml_section) -> None:
        with open(NEW_TABLE_REFINED_VIEWS_YAML_FILE_PATH, 'w') as file:
            file.write(yaml_section)
            logger.info(f"successfully wrote to temp refined views file")

    def create_refined_views_yaml(raw_txt) -> None:

        regex_expression = r" - name:(.*)"
        match = re.search(regex_expression, raw_txt, re.DOTALL)

        if match:
            yaml_section = match.group(0).rstrip('\n')
            write_temp_yaml_file(yaml_section)

        else:
            raise Exception(f"regex expression: {regex_expression} did not match")

    # Ensure the directory exists or create it
    os.makedirs(YML_MOD_OUTPUT_FOLDER, exist_ok=True)

    # write_raw_txt(raw_txt)
    create_refined_views_yaml(raw_txt)

def update_refined_views_yaml() -> None:

    def no_validated_tables_present() -> bool:
        if len(VALIDATED_TABLES) == 0:
            print("there are no tables to work with!!!")
            logger.warn("There are no validated tables to work with!")
            return True
        else:
            return False

    if no_validated_tables_present():
        return None
    
    missing_tables: list = get_missing_tables_in_refined_views()

    if len(missing_tables) == 0:
        print("there are no missing tables to add to refined views yaml!")
        return None

    command_list: list[str] = _generate_yml_gen_mod_command_list(missing_tables)

    raw_txt, response = execute_dbt_command(command_list)
 
    logger.info(f"raw txt output for refined yml is {raw_txt}")

    regex_expression = r" - name:(.*)"
    match = re.search(regex_expression, raw_txt, re.DOTALL)

    if match is None:
        print("""there was an issue executing the macro on dbt. Did you build all the models?  
              You need the models built first.  The command was ... {command_list}""")
        return None

    yaml_section = match.group(0).rstrip('\n')

    with open(NEW_TABLE_REFINED_VIEWS_YAML_FILE_PATH, 'w', encoding="utf-8") as file:
        file.write(yaml_section)

    with open(NEW_TABLE_REFINED_VIEWS_YAML_FILE_PATH, "r", encoding="utf-8") as source_file:
        new_yaml = source_file.read()

        with open(REFINED_VIEWS_PATH, "a", encoding="utf-8") as destination_file:
            destination_file.write('\n')
            destination_file.write('\n')
            destination_file.write(new_yaml)

    logger.info(f"successfully added new tables to clarity_sources.yml that were not already there: {missing_tables}")

if __name__ == '__main__':
    update_refined_views_yaml()