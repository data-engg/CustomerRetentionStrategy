package model

case class CaseRecord(case_no : Int,
                      create_timestamp : String,
                      last_modified_timestamp : String,
                      created_employee_key: Int,
                      call_center_id : String,
                      status : String,
                      category : String,
                      sub_category : String,
                      communication_mode : String,
                      country_cd : String,
                      product_code : Int)
