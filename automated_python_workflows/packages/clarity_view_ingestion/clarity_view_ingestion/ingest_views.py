

from clarity_view_ingestion.src.get_src_view_scripts import get_all_src_view_scripts
from clarity_view_ingestion.src.rebuild_views.rebuild_views_phase_one import phase_one_rebuild
from clarity_view_ingestion.src.rebuild_views.rebuild_views_phase_two import phase_two_rebuild
from clarity_view_ingestion.src.analyze_dependencies_for_views import create_view_dependency_mapping
from clarity_view_ingestion.src.rebuild_views.rebuild_views_phase_three import phase_three_rebuild


if __name__ == '__main__':
    get_all_src_view_scripts()

    #write the original query from source view scripts to a dbt sql file
    phase_one_rebuild()

    #move comment block to top of file, remove unnecessary view code at start of file before first SELECT statement
    #transform all text to lower case, remove semi colon at end of sql file if it exists
    phase_two_rebuild()

    create_view_dependency_mapping()
    
    #ONE LINE SUMMARY: adds CTEs to each query and removes unneeded table name references for the views that have all dependencies ingested
    #DETAILED SUMMARY:
        #starts with reading json output from phase2 rebuild as starting point
        #identifies all the table dependencies used in all the queries we want ingested via get_all_table_dependencies()
        #identifies all table names and table shorthands used in the query and creates a mapping of this information with create_master_table_ref_dict()
        #reads input for tables in "dependencies_ingested" input folder.  This will tell us what views we can ingest
        #for the views we can ingest...
            #create cte at top of query
            #replace the full table references like claritypoc..mytable to remove the "claritypoc.." part
    phase_three_rebuild()