package model

case class SurveyRecord(survey_id :String,
                        case_no : Int,
                        survey_timestamp : String,
                        q1 : String,
                        q2 : String,
                        q3 : String,
                        q4 : String,
                        q5 : String)