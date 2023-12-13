## CONFIGURATION SETUP
1. RECOMMENDED: create a virtual conda env
2. install dependencies

    - option 1: install everything from the [enviornment.yml](environment.yml) file
    - option 2: manually install from this list of dependencies
        - conda install pip
            - required
        - conda install pandas
            - required for automation workflow scripts
        - pip install dbt-snowflake
            - required to run dbt commands
        - pip install snowflake-connector-python
            - required to execute snowflake queries
        - pip install jupyterlab
            - optional for debugging/testing in a .ipynb file instead of a .py file
        - conda install -c conda-forge fastparquet
            - optional for viewing azuring landing zone parquet files (used for debugging occasionally)
        - pip install pyyaml (should already be installed)
            - experimenting for parsing through clarity_sources and refined_views yml files
        - 

3. install VS code extensions

    - dbt Power User

4. create profiles.yml file for executing dbt commands

    - cd into the proper directory
```
cd ~/.dbt/
```
    - create a file "profiles.yml" and insert the below information

```
default: 
outputs:
    dev:
    type: snowflake
    threads: 4
    account: advocate-mdp

    user: CALE.PLISKA@AAH.ORG
    authenticator: externalbrowser
    role: FR_MDP_DEVELOPER_DEV

    database: MDP_RAW_DEV
    warehouse: MDP_DEVELOPMENT_WH
    schema: dbt_cpliska

target: dev
```

5. modify dbt_project.yml file
    - Modify the dbt_project.yml file to remove the "if target.name = 'dev' then MDP_DEV" type logic because there is currently an issue with parsing this logic in the dbt project file
<details>
    <summary>dbt_project.yml</summary>

```
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'mdp'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: 'default'

# These configurations specify where dbt should look for different types of files.
# The `source-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
asset-paths: ["assets"]


target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"



on-run-end:
#  - "{{ dbt_artifacts.upload_results(results) }}"  #uncomment and comment out following line to debug and upload manifest in dev
  - "{% if target.name == 'prod' %}{{ dbt_artifacts.upload_results(results) }}{% elif target.name == 'test' %}{{ dbt_artifacts.upload_results(results) }}{% elif target.name == 'uat' %}{{ dbt_artifacts.upload_results(results) }}{% endif %}"  

vars:
  replace_underscore: false
  add_suffix: false
  suffix: 'suffix'
  add_prefix: false
  prefix: 'prefix'

  raw_database: "mdp_raw_dev" 
  #mdp_raw_dev #TODO will need to adjust for different dev,qa,prod environments
  raw_schema_list: ['clarity_aah'] #Update with every schema (datasource) that raw files will be landing into
  snowflake_storage_integration: "AZ_MDP_LANDING_INT"
  snowflake_notification_integration: "AZ_MDP_LANDING_NOTIFICATION"
  azure_blob_storage_url: "azure://stmdpprodeastus2001.blob.core.windows.net/mdp-landing/clarity{% if target.name == 'dev' %}nonprod{% elif target.name == 'default' %}nonprod{% elif target.name == 'test' %}nonprod{% elif target.name == 'uat' %}prod{% elif target.name == 'prod' %}prod{% endif %}.ahc.root.loc"

on-run-start: create or replace table {{var("raw_database")}}.GENERAL_MGMT.CURRENT_INVOCATION_ID (INVOCATION_ID string) AS select IFNULL('{{invocation_id}}','null')

# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{ config(...) }}` macro.
models:
  mdp:
    raw:
      materialized: table
      database: "mdp_raw_dev"
      
      #data_sources
      clarity_aah:
        schema: clarity_aah
        delete_handling:
          full_load:
            materialized: view
            #+post-hook: "{{watermark_load('default','raw')}}"
          incremental:
            materialized: incremental
            +post-hook: "{{watermark_load('default','raw')}}"
      general_mgmt:
        schema: general_mgmt
        dbt_execution_history:
          materialized: table
    
    refined:
      materialized: view
      database: "mdp_refined_dev"

      #data_sources
      clarity_aah:
        base_tables:
          #materialized: table
          schema: clarity_aah_base
          +transient: false

          full_load:
            materialized: incremental_custom
            +incremental_strategy: insert_overwrite
            +post-hook: "{{watermark_load('default','refined')}}"
            
          incremental:
            materialized: incremental
            +post-hook: "{{watermark_load('default','refined')}}"

        recreated_source_views:
          materialized: view
          schema: clarity_aah
        views:
          materialized: view
          schema: clarity_aah
          #+post-hook: "{{ insert_high_watermark('refined') }}"
    
    conformed:
      database: "mdp_conformed_dev"
      base_tables:
        materialized: table
        schema: enterprise_master_base
        +post-hook: "{{watermark_load('default','conformed')}}"
      views:
        materialized: view
        schema: enterprise_master  
    
    compliance-monitoring:
      materialized: table
      +transient: false
      database: MDP_AUDIT_MONITORING #"mdp_raw_{% if target.name == 'dev' %}dev{% elif target.name == 'test' %}test{% elif target.name == 'uat' %}uat{% elif target.name == 'prod' %}prod{% endif %}"
      
      #data_sources
      #platform_audit:
      #  schema: platform_audit 
      monitoring:
        schema: monitoring
      account_usage:
        schema: account_usage  
        

  dbt_artifacts:
    database: "mdp_raw_dev"
    schema: general_mgmt # optional, default is your target schema
    staging:
      database: "mdp_raw_dev"
      schema: general_mgmt # optional, default is your target schema
    sources:
      database: "mdp_raw_dev"
      schema: general_mgmt # optional, default is your target schema      

seeds:
  database: "mdp_raw_dev"
  schema: general_mgmt
```
</details>

## HOW TO USE

1. make sure your terminal is set to the proper directory.  You should be inside the MDP_DBT_Cloud remote repository.  


```cd  path-to-MDP-repo\MDP_DBT_Cloud```

2. to validate if you run ```ls``` you should see the macros, models, and seeds folders.

3. 

## How to update dependencies in repo

if you change the versions, you can run

```powershell
conda env export > environment.yml
```



remember to update the text-settings for pattern to point sql files to jinja stuff
