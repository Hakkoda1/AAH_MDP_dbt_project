import os
import logging

from typing import Dict

logger = logging.getLogger(os.path.basename(__file__))

from clarity_view_ingestion.src.utilities.common_functions import read_yaml_to_dict
from clarity_view_ingestion.src.config import PROJECT_OUTPUT_FOLDER, SRC_VIEW_SCRIPTS_OUTPUT_PATH

DBT_CREATE_VIEWS_OUTPUT_ORIGINALS_FOLDER = os.path.join(PROJECT_OUTPUT_FOLDER, 'originals')

def _write_original_views(views_and_queries: Dict[str, str], output_folder: str) -> None:
    os.makedirs(output_folder, exist_ok=True)
    for view, query in views_and_queries.items():
        if query is None:
            continue
        with open(os.path.join(output_folder, f"{view}.sql"), 'w') as f:
            f.write(query)

def phase_one_rebuild():
    views_and_queries = read_yaml_to_dict(SRC_VIEW_SCRIPTS_OUTPUT_PATH)
    _write_original_views(views_and_queries, DBT_CREATE_VIEWS_OUTPUT_ORIGINALS_FOLDER)

if __name__ == "__main__":
    phase_one_rebuild()