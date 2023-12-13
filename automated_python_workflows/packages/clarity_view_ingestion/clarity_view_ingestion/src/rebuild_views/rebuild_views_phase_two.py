import os
import re
import logging

from typing import Dict, Tuple

from clarity_view_ingestion.src.utilities.common_functions import read_yaml_to_dict, write_dict_to_json
from clarity_view_ingestion.src.config import (
    PROJECT_OUTPUT_FOLDER, 
    SRC_VIEW_SCRIPTS_OUTPUT_PATH, 
    CLEANED_VIEWS_QUERY_MAPPING_PATH
    )

logger = logging.getLogger(os.path.basename(__file__))

DBT_CREATE_VIEWS_OUTPUT_CLEANED_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, 'cleaned')

def transform_dict_to_lower(mapping: dict) -> dict:
    return {key.lower(): value.lower() for key, value in mapping.items() if value is not None}

def replace_matching_text(input_text, regex_pattern, replace_with='') -> Tuple[str, str]:
    """Remove the first occurrence of the regex pattern in the input text."""
    match = re.search(regex_pattern, input_text, re.DOTALL)
    if match:
        captured_text = match.group(0)
        # replaced_text = re.sub(regex_pattern, "", input_text, count=1, flags=re.DOTALL)
        replaced_text = input_text.replace(captured_text, replace_with).strip()
    else:
        replaced_text = input_text
        captured_text = ""
    return (replaced_text, captured_text)

def remove_txt_before_first_select_statement(query: str) -> str:
    regex = r'^(.*?)(?=select[^\w])'
    updated_query, _ = replace_matching_text(query, regex)
    return updated_query

def remove_comment_block(query: str) -> str:
    regex = r'\/\*(\*(?!\/)|[^*])*\*\/'
    updated_query, comment_block = replace_matching_text(query, regex)
    return comment_block, updated_query

def create_mapping_with_cleaned_query(views_and_queries: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    """
    takes in output from src_view_script_csv_to_dict() and generates a new dict.
    this new takes out unneeded parts from the query and places them separately in
    the dict if needed.
    """
    new_view_mapping = {}
    for view, query in views_and_queries.items():
        logger.debug(f"removing comment block for {view}")
        comment_block, updated_query = remove_comment_block(query)

        logger.debug(f"removing text before first select statement for {view}")
        updated_query = remove_txt_before_first_select_statement(updated_query)
        updated_query = updated_query.replace(";", "") #having semi-colon at end of dbt script will cause error

        new_view_mapping[view] = {
            "query": updated_query,
            "comment_block": comment_block
        }
    return new_view_mapping

def write_cleaned_views_sql_files(views_and_queries: Dict[str, Dict[str, str]]) -> None:
    os.makedirs(DBT_CREATE_VIEWS_OUTPUT_CLEANED_FOLDER, exist_ok=True)
    for view, query_dict in views_and_queries.items():
        query = query_dict['query']
        comment = query_dict['comment_block']
        with open(os.path.join(DBT_CREATE_VIEWS_OUTPUT_CLEANED_FOLDER, f"{view}.sql"), 'w') as f:
            f.write(comment)
            f.write('\n\n\n')
            f.write(query)

def phase_two_rebuild():
    views_and_queries = read_yaml_to_dict(SRC_VIEW_SCRIPTS_OUTPUT_PATH)
    views_and_queries_lower = transform_dict_to_lower(views_and_queries)
    cleaned_views_and_queries = create_mapping_with_cleaned_query(views_and_queries_lower)
    write_cleaned_views_sql_files(cleaned_views_and_queries)
    write_dict_to_json(cleaned_views_and_queries, CLEANED_VIEWS_QUERY_MAPPING_PATH)

if __name__ == "__main__":
    phase_two_rebuild()