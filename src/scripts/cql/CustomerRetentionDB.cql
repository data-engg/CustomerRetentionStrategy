USE edureka_735821;

-- Table to capture realtime case data
CREATE TABLE IF NOT EXISTS CASE_REALTIME(
case_no INT,
create_timestamp TIMESTAMP,last_modified_timestamp TIMESTAMP,created_employee_key INT,call_center_id TEXT,status TEXT,category TEXT,sub_category TEXT,communication_mode TEXT,country_cd TEXT,product_code INT,
row_insertion_dttm TIMESTAMP,
PRIMARY KEY (case_no));

-- Table to capture realtime survey data
CREATE TABLE IF NOT EXISTS SURVEYS_REALTIME(
survey_id TEXT,
case_no INT,
survey_timestamp TIMESTAMP,
q1 TEXT,
q2 TEXT,
q3 TEXT,
q4 TEXT,
q5 TEXT,
row_insertion_dttm TIMESTAMP,
PRIMARY KEY (survey_id, case_no));

-- Table to capture daily case data
CREATE TABLE IF NOT EXISTS CASE_DAILY(
case_no INT,
create_timestamp TIMESTAMP,
last_modified_timestamp TIMESTAMP,
created_employee_key INT,
call_center_id TEXT,
status TEXT,
category TEXT,
sub_category TEXT,
communication_mode TEXT,
country_cd TEXT,
product_code INT,
row_insertion_dttm TIMESTAMP,
PRIMARY KEY (case_no));

-- Table to capture daily survey data
CREATE TABLE IF NOT EXISTS SURVEYS_DAILY(
survey_id TEXT,
case_no INT,
survey_timestamp TIMESTAMP,
q1 TEXT,
q2 TEXT,
q3 TEXT,
q4 TEXT,
q5 TEXT,
row_insertion_dttm TIMESTAMP,
PRIMARY KEY (survey_id, case_no));
