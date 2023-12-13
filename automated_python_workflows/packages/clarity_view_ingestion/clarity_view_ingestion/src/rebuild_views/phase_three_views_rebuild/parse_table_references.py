import os
import re
import logging

from typing import Dict

import logging

logger = logging.getLogger(os.path.basename(__file__))

"""
this script cannot be ran by itself, it is executed through parse_query.py
"""

def get_chars_after_match(query: str, match: re.Match, num_of_chars_after: int) -> str:
    return query[match.end(): match.end() + num_of_chars_after]

def separate_new_lines(str) -> list[str]:
    return str.split('\n')

def analyze_first_word(query_partial: str) -> re.Match:
    regex = r'(\w+)'
    match = re.search(regex, query_partial)
    return  match

def analyze_second_word(query_partial: str) -> re.Match:
    regex = r'as\s*(\w+)'
    match = re.search(regex, query_partial)
    return  match

def get_table_shorthand(query: str, match: re.Match) -> str:
    chars_after_match: str = get_chars_after_match(query, match, 50)
    new_lines: list[str] = separate_new_lines(chars_after_match)
    first_word_match: re.Match = analyze_first_word(new_lines[0])
    if first_word_match:
        if first_word_match.group(1) == "on":
            return None
        if first_word_match.group(1) == "as":
            second_word_match: re.Match = analyze_second_word(new_lines[0])
            if second_word_match:
                return second_word_match.group(1)
        return first_word_match.group(1)
    else:
        return None
    
def get_table_refs(regex, view, query) -> Dict[str, Dict[str, str]]:
    table_ref_dict = {}
    matches = list(re.finditer(regex, query))

    if len(matches) == 0:
        return None
    
    for match in matches:
        full_reference = match.group(1)
        table_name = match.group(2)

        #shorthand
        if table_name in table_ref_dict.keys():
            new_shorthand = get_table_shorthand(query, match)
            
            if table_ref_dict[table_name]['table_shorthand'] != new_shorthand:
                if table_ref_dict[table_name]['full_reference'] != full_reference:
                    print(f"THERE IS A BIG PROBLEM WITH {view}...the author used the same table shorthand for two different tables")
                    logger.warn(f"THERE IS A BIG PROBLEM WITH {view}...the author used the same table shorthand for two different tables")
                prev_shorthand: list = table_ref_dict[table_name]['table_shorthand']
                prev_shorthand.append(new_shorthand)
                table_shorthand = prev_shorthand
        else:
            shorthand = get_table_shorthand(query, match)
            table_shorthand = [shorthand]
            table_shorthand_used_in_query = True

        if shorthand is None:
            table_shorthand = [table_name]
            table_shorthand_used_in_query = False

        #full_reference
        if full_reference == table_name:
            full_reference = ""
        
        # if table_shorthand is None:
        #     table_shorthand = [table_name]


        table_ref_dict[table_name] = {'full_reference': full_reference,
                                    'table_shorthand': table_shorthand,
                                    'table_shorthand_used_in_query': table_shorthand_used_in_query}

    return table_ref_dict

def create_table_ref_dict(view: str, query: str) -> Dict[str, str]:
    """
    for a view query generate a mapping where table dependency is the key.
    the value is a dict that includes the full_reference and shorthand reference.
    This is used when building out the proper dbt view scripts
    """

    table_ref_parse_strategies = {
        "clarity_poc_table_refs": r'(claritypoc\.\.(\w+))',
        "dbo_table_refs": r'(dbo\.(\w+))',
        "simple_table_ref": r'from\s*((\w*))\s*where'
    }

    for parse_strategy in table_ref_parse_strategies.keys():
        regex = table_ref_parse_strategies[parse_strategy]
        results = get_table_refs(regex, view, query)
        if results is not None:
            return results
        
    return {}

if __name__ == "__main__":
    pass