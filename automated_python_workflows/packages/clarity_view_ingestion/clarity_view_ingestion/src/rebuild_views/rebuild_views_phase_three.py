import os
import logging

from clarity_view_ingestion.src.utilities.common_functions import read_json_to_dict, write_dict_to_json
from clarity_view_ingestion.src.analyze_dependencies_for_views import get_views_with_all_dependencies_ingested

from clarity_view_ingestion.src.rebuild_views.phase_three_views_rebuild.parse_table_references import create_table_ref_dict
from clarity_view_ingestion.src.rebuild_views.phase_three_views_rebuild.rebuild_view_scripts import replace_table_refs

from clarity_view_ingestion.src.config import (
    CLEANED_VIEWS_QUERY_MAPPING_PATH, 
    PHASE3_VIEWS_QUERY_MAPPING_PATH, 
    PROJECT_OUTPUT_FOLDER)

logger = logging.getLogger(os.path.basename(__file__))

DBT_CREATE_VIEWS_OUTPUT_DRAFTS_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, 'drafts')

def write_draft_view(view: str, query: str, comment: str) -> None:
    os.makedirs(DBT_CREATE_VIEWS_OUTPUT_DRAFTS_FOLDER, exist_ok=True)
    with open(os.path.join(DBT_CREATE_VIEWS_OUTPUT_DRAFTS_FOLDER, f"{view}.sql"), 'w') as f:
        f.write(comment)
        f.write('\n\n\n')
        f.write(query)

def phase_three_rebuild() -> None:
    cleaned_views_and_queries = read_json_to_dict(CLEANED_VIEWS_QUERY_MAPPING_PATH)
    views_with_all_dependencies_ingested = get_views_with_all_dependencies_ingested()

    phase3_views_query_mapping = {}
    for view in views_with_all_dependencies_ingested:
        query = cleaned_views_and_queries[view]['query']
        if query is None:
            continue
        comment = cleaned_views_and_queries[view]['comment_block']
        table_ref_dict = create_table_ref_dict(view, query)
        updated_query = replace_table_refs(query, table_ref_dict)

        phase3_views_query_mapping[view] = {
            "query" : updated_query,
            "comment_block": comment}

        write_draft_view(view, updated_query, comment)

    write_dict_to_json(phase3_views_query_mapping, PHASE3_VIEWS_QUERY_MAPPING_PATH)

if __name__ == "__main__":
    phase_three_rebuild()
