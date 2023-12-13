import os
import logging

from clarity_table_ingestion.table_ingestion_validation import validate_new_tables
from clarity_table_ingestion.src.post_validation.fix_invalidated_input_tables import fix_invalidated_input_tables
from clarity_table_ingestion.src.post_validation.model_generator import generate_models
from clarity_table_ingestion.src.post_validation.dbt_build import build_models
from clarity_table_ingestion.src.post_validation.fixing_invalidated_input_tables.stage_external_sources import generate_command_to_stage_all_external_sources
from clarity_table_ingestion.src.post_validation.update_refined_views import update_refined_views_yaml

logger = logging.getLogger(os.path.basename(__file__))

if __name__ == '__main__':

    validate_new_tables()
    fix_invalidated_input_tables()
    validate_new_tables()

    generate_models()

    build_models()

    update_refined_views_yaml()

    generate_command_to_stage_all_external_sources()