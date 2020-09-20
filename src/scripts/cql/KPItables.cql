-- select keyspace
USE edureka_735821;

-- Table to capture realtime kpis
create table if not exists edureka_735821_crm_kpi_realtime(
case_status set<text>,
priority_severity set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch case kpis per hour
create table if not exists edureka_735821_crm_case_kpi_hourly(
all_cases int,
priority_wise_case_count set<text>,
case_by_status set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch case kpis per day
create table if not exists edureka_735821_crm_case_kpi_daily(
all_cases int,
priority_wise_case_count set<text>,
case_by_status set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch case kpis per week
create table if not exists edureka_735821_crm_case_kpi_weekly(
all_cases int,
priority_wise_case_count set<text>,
case_by_status set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch case kpis per month
create table edureka_735821_crm_case_kpi_monthly(
all_cases int,
priority_wise_case_count set<text>,
case_by_status set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch survey kpis per hour
create table edureka_735821_crm_survey_kpi_hourly(
total_survey int,
survey_resp_type_count set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch survey kpis per daily
create table edureka_735821_crm_survey_kpi_daily(
total_survey int,
survey_resp_type_count set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch survey kpis per week
create table edureka_735821_crm_survey_kpi_weekly(
total_survey int,
survey_resp_type_count set<text>,
ts timestamp,
primary key (ts));

-- Table to capture batch survey kpis per month
create table edureka_735821_crm_survey_kpi_monthly(
total_survey int,
survey_resp_type_count set<text>,
ts timestamp,
primary key (ts));