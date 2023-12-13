-- depends_on: {{ ref('column_config') }}
-- yml_col_gen('Clarity_POC',['LIST of values to exclude'])
{% macro yml_col_gen_refined_base(table,model_yaml) %}

    
    {% set column_metadata_query %}
        select 
            table_name,
            column_name,
            description,
            deprecated_yn
        from {{ref('column_config')}}
        where
            table_name = substr(replace(upper('{{ table }}'), 'LKP_CLR' , 'ZC' ), 0,  length(replace(upper('{{ table }}'), 'LKP_CLR' , 'ZC' ))-5) OR table_name = substr(upper('{{ table }}'), 0, length(upper('{{ table }}')) -5)
            {#substr(upper('lkp_clr_acct_basecls_ha_base'), 0, length(upper('lkp_clr_acct_basecls_ha_base')) -5)#}
        order by line, column_name
    {% endset %}

    {% set results = run_query(column_metadata_query) %}

    {% if execute %}
    {%- set column_list = results.columns[1].values() -%}
    {%- set deprecated_flag_list = results.columns[3].values() -%}
    {%- endif -%}

    {% do model_yaml.append('   columns:') %}
    {% for (column,deprecated_flag) in zip(column_list,deprecated_flag_list) %}
        {% do model_yaml.append('      - name: ' ~ column | lower) %}
        {% if deprecated_flag == 'Y' %}
            {#{% do model_yaml.append('        description: "' ~ description | replace('"','\\"') | replace(":","=")  ~ '"') %}#}
            {% do model_yaml.append('        meta:') %}
            {% do model_yaml.append('          database_tags:') %}
            {% do model_yaml.append('            clarity_deprecated_column: y') %}
        {% endif %}

    {%- endfor -%}

    {% do return(model_yaml) %}
{% endmacro %}


{% macro yml_gen_refined_base(tables_location_sf) %}

    {% set model_yaml=[] %}
    {%set refined_database =  var("refined_database")%}
    {% set refined_models_name %}
    {#show tables in schema {{tables_location_sf}} {# change to show refined..clarity_aah_base schema's tables #}
    with cte_tbls_usg as
    (
    select
    table_name
    from
    SNOWFLAKE.ACCOUNT_USAGE.TABLES
    where table_schema ilike  '%{{tables_location_sf}}%'  {#'%clarity_aah_base%'#}
    and table_catalog ilike '%{{refined_database}}%' {#'MDP_REFINED_DEV'#}
    and table_type = 'BASE TABLE'
    and deleted is null
    )
    select
    usg.table_name,
    lst.ingest_tag,
    lst.size_tag
    from cte_tbls_usg usg
    left join MDP_RAW_DEV.GENERAL_MGMT.INGESTION_LIST lst on usg.table_name=upper(lst.table_name)

    {% endset %}

    {% set results = run_query(refined_models_name) %}
    
    {% if execute %}
    {%- set refined_models_list = results.columns[0].values() -%} {#changing index from 1 to 0#}
    {%- set refined_ingest_tags_list = results.columns[1].values() -%} 
    {%- set refined_ingest_size_tags_list = results.columns[2].values() -%} 
    {%- endif -%}

    {% do model_yaml.append('version: 2') %}
    {% do model_yaml.append('') %}
    {% do model_yaml.append('models:') %}

        {# {%- for table in table_list -%} #}
        {% for (table,ingest_tag,size_tag) in zip(refined_models_list, refined_ingest_tags_list, refined_ingest_size_tags_list) %}
            {% do model_yaml.append('') %}
            {% do model_yaml.append(' - name: ' ~ table | lower) %}
            {% do model_yaml.append('   config: ' ) %}
            {# % do model_yaml.append('     tags: []' ) %}
            {# % do model_yaml.append('   tests: ' ) %}
            {% do model_yaml.append('      - check_clarity_row_count_against_source' ) %} #}
            {%- set tag_str = '     tags: ['%}
            {% if (ingest_tag is not none) and (size_tag is not none) %} 
                {%- set tag_str = tag_str ~ ingest_tag ~ ',' ~ size_tag%}
                
            {% elif (ingest_tag is none) and (size_tag is not none) %} 
                {%- set tag_str = tag_str ~ size_tag%}
                
                {%- endif -%}
                {%- set tag_str = tag_str ~ ']'%}
                {% do model_yaml.append(tag_str) %}
            {% set model_yaml = yml_col_gen_refined_base(table,model_yaml) %}
        {%- endfor -%}

        {% if execute %}

            {% set joined = model_yaml | join ('\n') %}
            {{ log(joined, info=True) }}
            {% do return(joined) %}

        {% endif %}
      

    --Loop through each table and fill in parameters
    /*
- name: clarity_emp
   config: 
     tags: []
   tests: 
      - check_clarity_row_count_against_source
   columns:
      - name: user_id
        description: "The unique ID assigned to the user record. This ID may be encrypted."
      - name: name
        description: "The name of the user record. This name may be hidden."
      - name: prov_id
        description: "The unique ID of the provider record that is linked to this user record if the user is a clinical system provider. This ID may be encrypted."
      - name: epic_emp_id
        description: "The unique ID assigned to the user record. This column will be omitted from public views of the CLARITY_EMP table."
      ...

    */
{% endmacro %}