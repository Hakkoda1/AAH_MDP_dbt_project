import subprocess

from ..global_utilities.common_functions import get_unique_tables_in_clarity_sources_manual 

from ..global_utilities.global_vars import CLARITY_SOURCES_MANUAL_PATH

YML_GEN_OUTPUT_PATH = r'python-workflow-scripts\cale version 3\full_yml_gen_refresh\output\raw_yml_gen_output.yml'


def _generate_yml_gen_command(exclude_list, yml_gen_output_path):

    tables_to_exclude = ','.join([f"'{table}'" for table in exclude_list])

    command = r'dbt run-operation yml_gen --args "{data_source_folder: ClarityPOC, exclude_list: [LIST_OF_TABLES]}" > YML_GEN_OUTPUT_PATH'
    command = command.replace("LIST_OF_TABLES", tables_to_exclude)
    command = command.replace("YML_GEN_OUTPUT_PATH", yml_gen_output_path)

    return command

if __name__ == '__main__':

    yml_gen_exclude_list = get_unique_tables_in_clarity_sources_manual(CLARITY_SOURCES_MANUAL_PATH)

    yml_gen_command = _generate_yml_gen_command(yml_gen_exclude_list, YML_GEN_OUTPUT_PATH)
    print(f'\n the yml_gen_command that will be executed is: {yml_gen_command}\n')

    subprocess.run(yml_gen_command.split(" "))

    #clean raw_yml_gen_output to -> _clarity_sources.yml
    #replace new _clarity_sources.yml with the existing on in the models folder
