-- USE DATABASE 
USE labuser_database;

-- CREATE DIM CALENDAR
CREATE TABLE IF NOT EXISTS DIM_CALENDAR(
calendar_date DATE,
date_desc VARCHAR(50),
week_day_nbr TINYINT,
week_number SMALLINT,
week_name VARCHAR(50),
year_week_number INT,
month_number TINYINT,
month_name VARCHAR(50),
quarter_number TINYINT,
quarter_name VARCHAR(50),
half_year_number TINYINT,
half_year_name VARCHAR(50),
geo_region_cd CHAR(2),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_DIM_CALENDAR PRIMARY KEY (calendar_date));

-- CREATE DIM CALL CENTER DETAILS
CREATE TABLE IF NOT EXISTS DIM_CALL_CENTER(
call_center_id VARCHAR(10),
call_center_vendor VARCHAR(50),
location VARCHAR(50),
country VARCHAR(50),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_DIM_CALL_CENTER PRIMARY KEY (call_center_id));

-- CREATE DIM CASE PRIORITY
CREATE TABLE IF NOT EXISTS DIM_CASE_PRIORITY(
Priority_key VARCHAR(5),
priority VARCHAR(20),
severity VARCHAR(100),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_DIM_CASE_PRIORITY PRIMARY KEY (Priority_key)); 

-- CREATE DIM CASE CATEGORY
CREATE TABLE IF NOT EXISTS DIM_CASE_CATEGORY(
category_key VARCHAR(10),
sub_category_key VARCHAR(10),
category_description VARCHAR(50),
sub_category_description VARCHAR(50),
priority VARCHAR(5),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_DIM_CASE_CATEGORY PRIMARY KEY (category_key),
CONSTRAINT FK_PRIORITY FOREIGN KEY (priority) REFERENCES DIM_CASE_PRIORITY(Priority_key) ON UPDATE CASCADE ON DELETE RESTRICT);

-- CREATE DIM CASE COUNTRY
CREATE TABLE IF NOT EXISTS DIM_CASE_COUNTRY(
id int,
Name VARCHAR(75),
Alpha_2 VARCHAR(2),
Alpha_3 VARCHAR(2),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_DIM_CASE_CATEGORY PRIMARY KEY (id));

-- CREATE DIM EMPLOYEE
CREATE TABLE IF NOT EXISTS DIM_EMPLOYEE(
emp_key INT,
first_name VARCHAR(50),
last_name VARCHAR(50),
email VARCHAR(50),
gender CHAR(1),
ldap VARCHAR(10),
hire_date DATE,
manager INT,
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_EMP_KEY PRIMARY KEY (emp_key),
CONSTRAINT FK_HIRE_DATE FOREIGN KEY (hire_date) REFERENCES DIM_CALENDAR(calendar_date) ON UPDATE CASCADE ON DELETE RESTRICT);

-- CREATE DIM PRODUCT 
CREATE TABLE IF NOT EXISTS DIM_PRODUCT(
product_id INT,
department VARCHAR(50),
brand VARCHAR(50),
commodity_desc VARCHAR(100),
sub_commodity_desc VARCHAR(100),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_PRODUCT_ID PRIMARY KEY (product_id));

-- CREATE DIM SURVEY
CREATE TABLE IF NOT EXISTS SURVEY_LKP(
question_id VARCHAR(5),
question_desc VARCHAR(200),
response_type VARCHAR(20),
response_range VARCHAR(10),
negative_response_range VARCHAR(10),
neutral_response_range VARCHAR(10),
positive_response_range VARCHAR(10),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_QUESTION_ID PRIMARY KEY (question_id));

-- CREATE FACT CASE
CREATE TABLE IF NOT EXISTS FACT_CASE(
case_no INT,
create_timestamp TIMESTAMP,
last_modified_timestamp TIMESTAMP,
created_employee_key INT,
call_center_id VARCHAR(10),
status VARCHAR(10),
category VARCHAR(10),
sub_category VARCHAR(10),
communication_mode VARCHAR(50),
country_cd INT,
product_code INT,
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_FACT_CASE PRIMARY KEY (case_no, status), 
CONSTRAINT FK_CREATED_EMPLOYEE FOREIGN KEY (created_employee_key) REFERENCES DIM_EMPLOYEE(emp_key) ON UPDATE CASCADE ON DELETE SET NULL,
CONSTRAINT FK_CALL_CENTER FOREIGN KEY (call_center_id) REFERENCES DIM_CALL_CENTER(call_center_id) ON UPDATE CASCADE ON DELETE RESTRICT,
CONSTRAINT FK_CATEGORY FOREIGN KEY (category) REFERENCES DIM_CASE_CATEGORY(category_key) ON UPDATE CASCADE ON DELETE RESTRICT,
CONSTRAINT FK_COUNTRY_CD FOREIGN KEY (country_cd) REFERENCES DIM_CASE_COUNTRY(id) ON UPDATE CASCADE ON DELETE RESTRICT, 
CONSTRAINT FK_PRODUCT FOREIGN KEY (product_code) REFERENCES DIM_PRODUCT (product_id) ON UPDATE CASCADE ON DELETE RESTRICT);

-- CREATE FACT SURVEY
CREATE TABLE IF NOT EXISTS FACT_SUREY(
survey_id VARCHAR(10),
case_no INT,
Q1 VARCHAR(2),
Q2 VARCHAR(2),
Q3 VARCHAR(2),
Q4 VARCHAR(2),
Q5 VARCHAR(2),
row_insertion_dttm timestamp DEFAULT now(),
CONSTRAINT PK_SURVEY_ID PRIMARY KEY (survey_id),
CONSTRAINT FK_CASE_NO FOREIGN KEY (case_no) REFERENCES FACT_CASE(case_no) ON UPDATE CASCADE ON DELETE CASCADE);