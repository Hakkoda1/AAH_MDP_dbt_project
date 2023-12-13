import os
import logging

from typing import Dict, List

logger = logging.getLogger(os.path.basename(__file__))

def create_cte_str(reference_name: str, underlying_model_name: str) -> str:
    underlying_model = underlying_model_name.replace('zc_','lkp_clr_')
    cte_format = "TABLE_SHORTHAND as (\n\tselect * from {{ ref('TABLE_NAME_base') }}\n)"
    cte_format = cte_format.replace('TABLE_SHORTHAND', reference_name)
    cte_format = cte_format.replace('TABLE_NAME', underlying_model)
    return cte_format

def add_dbt_ctes(query: str, table_ref_dict: Dict[str, Dict[str, str]]) -> str:
    full_cte_st = "with \n\n"
    cte_components: List[str] = []
    for table in table_ref_dict.keys():
        #add a cte for the table name itself because sometimes the views do not use shorthands
        cte_str = create_cte_str(table, table)
        cte_components.append(cte_str)
        list_of_shorthands_used = table_ref_dict[table]["table_shorthand"]
        if (len(list_of_shorthands_used) != 0):
            for table_shorthand in list_of_shorthands_used:
                if table_shorthand is None:
                    continue
                #add a cte for each shorthand used
                cte_str = create_cte_str(table_shorthand, table)
                cte_components.append(cte_str)

    full_cte_st += ",\n\n".join(cte_components)
    updated_query = full_cte_st + '\n' + query

    return updated_query

def remove_full_table_references(query: str) -> str:
    updated_query = query.replace("claritypoc..", "")
    return updated_query

def replace_table_refs(query: str, table_ref_dict: Dict[str, Dict[str, str]]) -> str:
    updated_query = add_dbt_ctes(query, table_ref_dict)
    updated_query = remove_full_table_references(updated_query)
    return updated_query

if __name__ == "__main__":
    pass