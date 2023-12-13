import os
import logging

from typing import Dict, List

from ingestion_utilities.processing_files import process_single_csv_input_from_folder
from clarity_view_ingestion.src.utilities.common_functions import read_json_to_dict, write_dict_to_json
from clarity_view_ingestion.src.phase_three_views_rebuild.parse_table_references import create_table_ref_dict

from clarity_view_ingestion.src.config import (
    PROJECT_INPUT_FOLDER, 
    CLEANED_VIEWS_QUERY_MAPPING_PATH, 
    MASTER_TABLE_REF_DICT_OUTPUT_PATH, 
    DEPENDENCY_MAPPING_PATH
    )

logger = logging.getLogger(os.path.basename(__file__))

DEPENDENCIES_INGESTED_FOLDER = os.path.join(PROJECT_INPUT_FOLDER, 'dependencies_ingested')

def create_master_table_ref_dict() -> Dict[str, Dict[str, any]]:
    cleaned_views_and_queries = read_json_to_dict(CLEANED_VIEWS_QUERY_MAPPING_PATH)
    master = {}
    for view in cleaned_views_and_queries.keys():
        query = cleaned_views_and_queries[view]['query']
        if query is None:
            continue
        table_ref_dict = create_table_ref_dict(view, query)
        master[view] = table_ref_dict
    write_dict_to_json(master, MASTER_TABLE_REF_DICT_OUTPUT_PATH)
    return master

def get_all_table_dependencies() -> List[str]:
    get_all_table_dependencies = []
    master_table_ref_dict = create_master_table_ref_dict()
    for _, create_table_ref_dict in master_table_ref_dict.items():
        dependencies_for_view = list(create_table_ref_dict.keys())
        get_all_table_dependencies.extend(dependencies_for_view)
        
    get_all_table_dependencies = set(get_all_table_dependencies)
    print(f"all the table dependences for the views are {get_all_table_dependencies}\n\n")
    return get_all_table_dependencies

def create_view_dependency_mapping() -> Dict[str, List[str]]:
    master_table_ref_dict = create_master_table_ref_dict()
    _, dependency_tables_currently_ingested = process_single_csv_input_from_folder(DEPENDENCIES_INGESTED_FOLDER)
    dependency_tables_currently_ingested = [table.lower() for table in dependency_tables_currently_ingested]

    dependency_mapping = {}
    for view in master_table_ref_dict.keys():
        dependency_mapping[view] = {
            "dependencies_not_ingested": []
        }
        for table_dependency in list(master_table_ref_dict[view].keys()):
            if table_dependency not in dependency_tables_currently_ingested:
                dependency_mapping[view]["dependencies_not_ingested"].append(table_dependency)
    write_dict_to_json(dependency_mapping, DEPENDENCY_MAPPING_PATH)
    
    return dependency_mapping

def get_views_with_all_dependencies_ingested():
    dependency_mapping = create_view_dependency_mapping()
    views_with_all_dependencies_ingested = []
    for view in dependency_mapping.keys():
        if len(dependency_mapping[view]["dependencies_not_ingested"]) == 0:
            views_with_all_dependencies_ingested.append(view)
    return views_with_all_dependencies_ingested

if __name__ == "__main__":
    get_all_table_dependencies()
    create_view_dependency_mapping()