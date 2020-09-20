-- USE DATABASE 
USE LABUSER_DATABASE;

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CALENDAR_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CALENDAR_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CALL_CENTER_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CALL_CENTER_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CASE_PRIORITY_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CASE_PRIORITY_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CASE_CATEGORY_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CASE_CATEGORY_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CASE_COUNTRY_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CASE_COUNTRY_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_EMPLOYEE_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_EMPLOYEE_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_PRODUCT_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_PRODUCT_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_SURVEY_QUESTION_DETAILS_LAST_MODIFIED
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_SURVEY_QUESTION_DETAILS_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR FACT_CASE
CREATE TABLE IF NOT EXISTS FACT_CASE_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR FACT_SURVEY
CREATE TABLE IF NOT EXISTS FACT_SURVEY_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CASE_DAILY_LAST_MODIFIED (CASSANDRA)
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CASE_DAILY_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_SURVEYS_DAILY_LAST_MODIFIED (CASSANDRA)
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_SURVEYS_DAILY_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_CASE_REALTIME_LAST_MODIFIED (CASSANDRA)
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_CASE_REALTIME_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR EDUREKA_735821_FUTURECART_SURVEYS_REALTIME_LAST_MODIFIED (CASSANDRA)
CREATE TABLE IF NOT EXISTS EDUREKA_735821_FUTURECART_SURVEYS_REALTIME_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- LAST MODIFIED TABLE FOR CRM_PIVOT (HIVE)
CREATE TABLE IF NOT EXISTS CRM_PIVOT_LAST_MODIFIED(
LAST_MODIFIED_DATE DATE,
LAST_MODIFIED_TS TIMESTAMP);

-- PROCEDURE TO UPDATE LAST_MODIFIED TABLE
DROP PROCEDURE UPDATE_LAST_MODIFIED_DATE;
DELIMITER //
CREATE PROCEDURE UPDATE_LAST_MODIFIED_DATE (IN TAB_NAME VARCHAR(25))
BEGIN
	SET @Q1 = CONCAT('TRUNCATE ', TAB_NAME, '_LAST_MODIFIED');
    SET @Q2 = CONCAT('INSERT INTO ',TAB_NAME, '_LAST_MODIFIED SELECT DATE(MAX(ROW_INSERTION_DTTM)) LAST_MODIFIED_DATE, MAX(ROW_INSERTION_DTTM) LAST_MODIFIED_TS FROM ', TAB_NAME);
    PREPARE TRUNCATE_COMM FROM @Q1;
    EXECUTE TRUNCATE_COMM;
    DEALLOCATE PREPARE TRUNCATE_COMM;
    PREPARE INSERT_COMM FROM @Q2;   
    EXECUTE INSERT_COMM;
    DEALLOCATE PREPARE INSERT_COMM;    
END//

-- PROCEDURE TO UPDATE LAST_MODIFIED TABLE ON THE BASIS OF A DIFFERENT TABLE 
DROP PROCEDURE UPDATE_LAST_MODIFIED_DATE;
DELIMITER //
CREATE PROCEDURE UPDATE_LAST_MODIFIED_DATE_T2 (IN BASE_TABLE VARCHAR(25), IN TARGET_TABLE VARCHAR(25))
BEGIN
	SET @Q1 = CONCAT('TRUNCATE ', TARGET_TABLE);
    SET @Q2 = CONCAT('INSERT INTO ',TARGET_TABLE , ' SELECT DATE(MAX(ROW_INSERTION_DTTM)) LAST_MODIFIED_DATE, MAX(ROW_INSERTION_DTTM) LAST_MODIFIED_TS FROM ', BASE_TABLE);
    PREPARE TRUNCATE_COMM FROM @Q1;
    EXECUTE TRUNCATE_COMM;
    DEALLOCATE PREPARE TRUNCATE_COMM;
    PREPARE INSERT_COMM FROM @Q2;   
    EXECUTE INSERT_COMM;
    DEALLOCATE PREPARE INSERT_COMM;    
END//