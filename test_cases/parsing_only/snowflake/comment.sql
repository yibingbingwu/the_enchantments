 -- Additional SQL allows us to set variables for each data_type/source_table query window, used in step1_* queries
-- {additional}
create table if not exists _bingwu_u21_job_log comment='This is a crude job management table to manage data transfer to Unit21' (
  job_id number,
  unit21_job_environment varchar,
  job_status varchar,
  data_type varchar,
  source_table varchar,
  query_window_start timestamp,
  query_window_end timestamp,
  actual_window_start timestamp,
  actual_window_end timestamp,
  num_objects number,
  job_start_time timestamp,
  job_end_time timestamp,
  unit21_environment varchar
);
