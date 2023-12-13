{% macro slim_ci_database_ddl_test(action) %}


    {% if action == 'create' %}
        

        {%- set create_raw -%}
            create database if not exists mdp_raw_test_clone clone mdp_raw_test
        {%- endset -%}
        {%- set results = run_query(create_raw) -%}

        {%- set create_refined -%}
            create database if not exists mdp_refined_test_clone clone mdp_refined_test
        {%- endset -%}
        {%- set results = run_query(create_refined) -%}

        {%- set create_conformed -%}
            create  database if not exists mdp_conformed_test_clone clone mdp_conformed_test
        {%- endset -%}
        {%- set results = run_query(create_conformed) -%}

    {% elif action == 'drop' %}

        {%- set drop_raw -%}
            drop database if exists mdp_raw_test_clone
        {%- endset -%}
        {%- set results = run_query(drop_raw) -%}

        {%- set drop_refined -%}
            drop database if exists mdp_refined_test_clone
        {%- endset -%}
        {%- set results = run_query(drop_refined) -%}

        {%- set drop_conformed -%}
            drop database if exists mdp_conformed_test_clone
        {%- endset -%}
        {%- set results = run_query(drop_conformed) -%}

    {% endif %}


{% endmacro %}
