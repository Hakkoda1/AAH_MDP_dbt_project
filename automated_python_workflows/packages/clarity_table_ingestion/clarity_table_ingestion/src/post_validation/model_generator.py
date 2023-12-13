import os 
import re
import csv
import logging

from clarity_table_ingestion.src.utilities.common_functions import get_validation_csv_path

VALIDATION_FILE_PATH = get_validation_csv_path()

logger = logging.getLogger(os.path.basename(__file__))

CURRENT_DIRECTORY = os.getcwd()

MODEL_OUTPUT_FOLDER_RAW_INC = os.path.join(CURRENT_DIRECTORY, r"models\raw\clarity_aah\delete_handling\incremental")
MODEL_OUTPUT_FOLDER_RAW_FULL_LOAD = os.path.join(CURRENT_DIRECTORY, r"models\raw\clarity_aah\delete_handling\full_load")
MODEL_OUTPUT_FOLDER_REFINED_INC = os.path.join(CURRENT_DIRECTORY, r"models\refined\clarity_aah\base_tables\incremental")
MODEL_OUTPUT_FOLDER_REFINED_FULL_LOAD = os.path.join(CURRENT_DIRECTORY, r"models\refined\clarity_aah\base_tables\full_load")
MODEL_OUTPUT_FOLDER_REFINED_VIEWS = os.path.join(CURRENT_DIRECTORY, r"models\refined\clarity_aah\views")

MODEL_NAME = 'Table_name'
LOAD_TYPE = 'Load_type'

def _process_views(file_path, model, output_folder):

    def generate_view_code(model_name):
        template_code = """
    {% set clarity_table_name = "NAME_OF_TABLE" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    """

        view_code = ""
        model = re.sub(r'^zc_', 'lkp_clr_', model_name)  
        replaced_code = template_code.replace("NAME_OF_TABLE", model)
        view_code += replaced_code + "\n"

        return view_code

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            if row['Ready_to_create_model_files'] == 'YES':
                model_name = row[model].lower()
                file_name = f'{model_name}.sql'
                file_name = re.sub(r'^zc_', 'lkp_clr_', file_name)
                file_code = generate_view_code(model_name)
                output_path = os.path.join(output_folder, file_name)
                with open(output_path, 'w') as output_file:
                    output_file.write(f'{file_code}\n')

def _process_refined(file_path, model, type, output_folder_refined_inc, output_folder_refined_full):

    def generate_refined_code(model_name, model_type, primary_keys):

        pimrary_keys_str = ','.join([f"'{primary_key}'" for primary_key in primary_keys])

        full_load_code = """
    {% set clarity_table_name = "NAME_OF_TABLE" %}

    {{ clarity_refined_base_full_load_table(clarity_table_name=clarity_table_name) }}
    """

        incremental_code = """
    {% set clarity_table_name = "NAME_OF_TABLE" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{
        config(
            materialized='incremental',
            unique_key=[NAME_OF_PRIMARY_KEYS]
        )
    }}

    {{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
    """

        view_code = ""
        if model_type.lower() == 'full':
            replaced_code = full_load_code.replace("NAME_OF_TABLE", model_name)
            view_code += replaced_code + "\n"

        elif model_type.lower() == 'incremental':
            replaced_code = incremental_code.replace("NAME_OF_TABLE", model_name).replace("NAME_OF_PRIMARY_KEYS", pimrary_keys_str)
            view_code += replaced_code + "\n"
        else:
            pass 

        return view_code

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Ready_to_create_model_files'] == 'YES':
                model_name = row[model].lower()
                model_type = row[type].lower()
                primary_keys = row["Primary_keys"].lower().split(" ") #will be used in future.
                file_name = f'{model_name}_base.sql'
                file_name = re.sub(r'^zc_', 'lkp_clr_', file_name)
                file_code = generate_refined_code(model_name, model_type, primary_keys)

                if model_type ==  'incremental':
                    output_path = os.path.join(output_folder_refined_inc, file_name)
                elif model_type ==  'full':
                    output_path = os.path.join(output_folder_refined_full, file_name)
                else:
                    raise Exception("the model is type is not incremental or full_load")

                with open(output_path, 'w') as output_file:
                    output_file.write(f'{file_code}\n')

def _process_raw(file_path, model, type, output_folder_raw_inc, output_folder_raw_full):

    def generate_raw_code(model_name, model_type):
        full_load_code = """
    {%- set source_model_name = "NAME_OF_TABLE" -%}
    {%- set instance_name = "clarity_aah" -%}

    {{ clarity_full_load_delete_handling(source_model_name=source_model_name,instance_name=instance_name) }}
    """

        incremental_code = """
    {% set clarity_table_name = "NAME_OF_TABLE" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}
    """

        view_code = ""
        if model_type.lower() == 'full':
            replaced_code = full_load_code.replace("NAME_OF_TABLE", model_name)
            view_code += replaced_code + "\n"

        elif model_type.lower() == 'incremental':
            replaced_code = incremental_code.replace("NAME_OF_TABLE", model_name)
            view_code += replaced_code + "\n"
        else:
            pass 

        return view_code

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Ready_to_create_model_files'] == 'YES':
                model_name = row[model].lower()
                model_type = row[type].lower()
                file_name_inc = f'raw_{model_name}_src_operation_history.sql'
                file_name_full_load = f'raw_{model_name}_latest.sql'

                file_code = generate_raw_code(model_name, model_type)

                if model_type ==  'incremental':
                    output_path = os.path.join(output_folder_raw_inc, file_name_inc)
                elif model_type ==  'full':
                    output_path = os.path.join(output_folder_raw_full, file_name_full_load)
                else:
                    raise Exception(f"the model {model_name} is type is not incremental or full_load")

                with open(output_path, 'w') as output_file:
                    output_file.write(f'{file_code}\n')

def generate_models() -> None:
    logger.info("model_generator started")

    _process_views(VALIDATION_FILE_PATH, MODEL_NAME, MODEL_OUTPUT_FOLDER_REFINED_VIEWS)
    _process_refined(VALIDATION_FILE_PATH, MODEL_NAME, LOAD_TYPE, MODEL_OUTPUT_FOLDER_REFINED_INC, MODEL_OUTPUT_FOLDER_REFINED_FULL_LOAD)
    _process_raw(VALIDATION_FILE_PATH, MODEL_NAME, LOAD_TYPE, MODEL_OUTPUT_FOLDER_RAW_INC, MODEL_OUTPUT_FOLDER_RAW_FULL_LOAD)

    logger.info("model_generator completed")

if __name__ == '__main__':
    generate_models()
