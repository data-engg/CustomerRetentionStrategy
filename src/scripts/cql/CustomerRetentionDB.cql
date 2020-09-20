use edureka_735821;

-- table to capture realtime case data
create table if not exists edureka_735821_futurecart_case_realtime(
case_no int,
create_timestamp timestamp,last_modified_timestamp timestamp,created_employee_key int,call_center_id text,status text,category text,sub_category text,communication_mode text,country_cd text,product_code int,
row_insertion_dttm timestamp,
primary key (case_no, last_modified_timestamp));

-- table to capture realtime survey data
create table if not exists edureka_735821_futurecart_surveys_realtime(
survey_id text,
case_no int,
survey_timestamp timestamp,
q1 text,
q2 text,
q3 text,
q4 text,
q5 text,
row_insertion_dttm timestamp,
primary key (survey_id, case_no));

-- table to capture daily case data
create table if not exists edureka_735821_futurecart_case_daily(
case_no int,
create_timestamp timestamp,
last_modified_timestamp timestamp,
created_employee_key int,
call_center_id text,
status text,
category text,
sub_category text,
communication_mode text,
country_cd text,
product_code int,
row_insertion_dttm timestamp,
primary key (case_no, last_modified_timestamp));

-- table to capture daily survey data
create table if not exists edureka_735821_futurecart_surveys_daily(
survey_id text,
case_no int,
survey_timestamp timestamp,
q1 text,
q2 text,
q3 text,
q4 text,
q5 text,
row_insertion_dttm timestamp,
primary key (survey_id, case_no));
