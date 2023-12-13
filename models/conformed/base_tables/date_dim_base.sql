{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite',
        post_hook = [ "{{insert_default_key_to_dim_table( 'date_dim_base', 
                                                          'date_key' )}}" ]
    )
}}


select

    {# keys #}
    replace(to_date(dat.calendar_dt) , '-' , '' ) as date_key, 

    {# dimensions #}
    dat.calendar_dt::datetime as calendar_dt,
    dat.day_of_week::varchar as day_of_week,
    dat.week_number::integer as week_number,
    dat.week_ending_dt::datetime as week_ending_dt,
    dat.last_friday_dt::datetime as last_friday_dt,
    dat.month_end_dt::datetime as month_end_dt,
    dat.day_of_month::integer as day_of_month,
    dat.month_name::varchar as month_name,
    dat.month_number::integer as month_number,
    dat.quarter_number::integer as quarter_number,
    dat.day_of_year::integer as day_of_year,
    dat.epic_dte::integer as epic_dte,
    dat.epic_dat::integer as epic_dat,
    dat.instant_at_midnight::numeric(18) as instant_at_midnight,
    dat.year::integer as year,
    dat.occurrence_in_month::integer as occurrence_in_month,
    dat.tomorrow_dt::datetime as tomorrow_dt,
    dat.year_month::varchar as year_month,
    dat.weekend_yn::varchar as weekend_yn,
    dat.quarter_begin_dt::datetime as quarter_begin_dt,
    dat.quarter_end_dt::datetime as quarter_end_dt,
    dat.same_day_year_ago::datetime as same_day_year_ago,
    dat.prev_day_dt::datetime as prev_day_dt,
    dat.leap_year_yn::varchar as leap_year_yn,
    dat.day_of_the_week_c::integer as day_of_the_week_c,
    dat.year_of_the_week::integer as year_of_the_week,
    dat.year_begin_dt::datetime as year_begin_dt,
    dat.month_begin_dt::datetime as month_begin_dt,
    dat.year_begin_dt_str::varchar as year_begin_dt_str,
    dat.month_begin_dt_str::varchar as month_begin_dt_str,
    dat.calendar_dt_str::varchar as calendar_dt_str,
    dat.qtr_begin_dt_str::varchar as qtr_begin_dt_str,
    dat.week_begin_dt::datetime as week_begin_dt,
    dat.week_begin_dt_str::varchar as week_begin_dt_str,
    dat.day_of_week_index::integer as day_of_week_index,
    dat.holiday_yn::varchar as holiday_yn,
    dat.monthname_year::varchar as monthname_year,
    dat.year_quarter::varchar as year_quarter,
    dat.year_month_str::varchar as year_month_str,
    dat.quarter_str::varchar as quarter_str,
    dat.bus_day_ct::integer as bus_day_ct,
    dat.weekday_ct::integer as weekday_ct,
    dat.usa_fiscal_year_begin_dt::datetime as usa_fiscal_year_begin_dt,
    dat.usa_fiscal_year_end_dt::datetime as usa_fiscal_year_end_dt,
    dat.year_end_dt::datetime as year_end_dt,
    dat.which_days_c::integer as which_days_c,
    dat.month_end_yn::varchar as month_end_yn,
    src_update_dt,

    {# metadata #}
    current_timestamp as mdp_effective_date,
    'clarity.dbo.date_dimension' as record_source 

from {{ ref("date_dimension_base") }} dat
