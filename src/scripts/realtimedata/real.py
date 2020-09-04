from random import randrange
from datetime import timedelta
from datetime import datetime
import  random
import calendar,errno
import time,os
import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--outputLocation', type=str, default="None")
args = parser.parse_args()

# retreiving config file name
outputLocation = args.outputLocation


if outputLocation == "None":
    survey_output_loc = "survey"
    case_output_loc = 'case'

else :
    survey_output_loc = outputLocation + "/survey"
    case_output_loc = outputLocation + "/case"

def createDir(dirName):
    try:
        os.makedirs(dirName)
    except OSError:
        if not os.path.isdir(dirName):
            raise

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


createDir(case_output_loc)
createDir(survey_output_loc)

open_case_time_diff_mins = [40,50,60]
closed_case_time_diff_mins = [5,10,20,30]
number_of_cases_counts = [1,2,3,4,5,6]
scores = [i for i in range (1,11)]
answer = ["Y","N"]
survey_id_start = 500000
category = 'CAT3'
sub_categorys = ['SCAT8','SCAT9','SCAT10','SCAT11','SCAT12','SCAT13','SCAT14','SCAT15','SCAT16']
with open('realtimedata/000000_0','r') as case_data_obj:
  all_case_data_lines = case_data_obj.readlines()

print len(all_case_data_lines)

total_cases = len(all_case_data_lines)

i = 900

while i <= (total_cases-1):
    sub_category  = random.choice(sub_categorys)
    number_of_cases = random.choice(number_of_cases_counts)
    print number_of_cases
    cases = all_case_data_lines[i:i+number_of_cases]
    current_timestamp = datetime.now()
    case_created_ts = str(current_timestamp - timedelta(minutes=random.choice(open_case_time_diff_mins)))[0:19]
    case_closed_ts = str(current_timestamp - timedelta(minutes=random.choice(closed_case_time_diff_mins)))[0:19]
    survey_ts = str(current_timestamp)[0:19]
    file_ts = calendar.timegm(time.gmtime())
    print cases
    cases_array = []
    cases_json = {}
    survey_array=[]
    survey_json={}
    if i%5 != 0:
        for j in cases:
            cases_data_dict = {}
            case_no,created_employee,call_center,status,category1,sub_category1,mode,country,product = j.replace('\n','').split(',')
            cases_data_dict["case_no"]=case_no
            cases_data_dict["created_employee_key"] = created_employee
            cases_data_dict["call_center_id"] = call_center
            cases_data_dict["status"] = status
            cases_data_dict["category"] = category
            cases_data_dict["sub_category"] = sub_category
            cases_data_dict["communication_mode"] = mode
            cases_data_dict["country_cd"] = country
            cases_data_dict["product_code"] = product
            cases_data_dict["last_modified_timestamp"] = case_created_ts
            cases_data_dict["create_timestamp"] = case_created_ts
            print cases_data_dict
            cases_array.append(cases_data_dict)

        print cases_array
        #cases_json["records"] = cases_array
        case_file_name = case_output_loc + "/case_data_" + str(file_ts) + ".json"
        print case_file_name
        print cases_json
        with open(case_file_name, 'w') as file:
            json_string = json.dumps(cases_array)
            file.write(json_string)

    elif i%5 == 0:
        for j in cases:
            cases_data_dict = {}
            case_no,created_employee,call_center,status,category1,sub_category1,mode,country,product = j.replace('\n','').split(',')
            cases_data_dict["case_no"]=case_no
            cases_data_dict["created_employee_key"] = created_employee
            cases_data_dict["call_center_id"] = call_center
            cases_data_dict["status"] = status
            cases_data_dict["category"] = category
            cases_data_dict["sub_category"] = sub_category
            cases_data_dict["communication_mode"] = mode
            cases_data_dict["country_cd"] = country
            cases_data_dict["product_code"] = product
            cases_data_dict["last_modified_timestamp"] = case_created_ts
            cases_data_dict["create_timestamp"] = case_created_ts
            print cases_data_dict
            cases_array.append(cases_data_dict)
        for j in cases:
            cases_data_dict = {}
            case_no,created_employee,call_center,status,category1,sub_category1,mode,country,product = j.replace('\n','').split(',')
            cases_data_dict["case_no"]=case_no
            cases_data_dict["created_employee_key"] = created_employee
            cases_data_dict["call_center_id"] = call_center
            cases_data_dict["status"] = 'Closed'
            cases_data_dict["category"] = category
            cases_data_dict["sub_category"] = sub_category
            cases_data_dict["communication_mode"] = mode
            cases_data_dict["country_cd"] = country
            cases_data_dict["product_code"] = product
            cases_data_dict["last_modified_timestamp"] = case_closed_ts
            cases_data_dict["create_timestamp"] = case_created_ts
            print cases_data_dict
            cases_array.append(cases_data_dict)
        print cases_array
        #cases_json["records"] = cases_array
        case_file_name = case_output_loc + "/case_data_" + str(file_ts) + ".json"
        print cases_json
        with open(case_file_name, 'w') as file:
            json_string = json.dumps(cases_array)
            file.write(json_string)
        for j in cases:
            case_no, created_employee, call_center, status, category1, sub_category1, mode, country, product = j.replace('\n', '').split(',')
            survey_data_dict = {}
            survey_id = "S-" + str(survey_id_start)
            survey_data_dict["survey_id"]=survey_id
            survey_data_dict["case_no"] = case_no
            survey_data_dict["survey_timestamp"] = survey_ts
            survey_data_dict["Q1"] = scores[random.randint(0, len(scores) - 1)]
            survey_data_dict["Q2"] = scores[random.randint(0, len(scores) - 1)]
            survey_data_dict["Q3"] = scores[random.randint(0, len(scores) - 1)]
            survey_data_dict["Q4"] = answer[random.randint(0, len(answer) - 1)]
            survey_data_dict["Q5"] = scores[random.randint(0, len(scores) - 1)]
            print survey_data_dict
            survey_array.append(survey_data_dict)
            survey_id_start = survey_id_start + 1
        print survey_array
        #survey_json["surveys"] = survey_array
        survey_file_name = survey_output_loc + "/survey_data_" + str(file_ts) + ".json"
        print survey_json
        with open(survey_file_name, 'w') as file:
            json_string = json.dumps(survey_array)
            file.write(json_string)
    i = i + number_of_cases
    time.sleep(5)

