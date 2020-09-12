package processing.stream

import utils.Utilities
import model.CaseRecord
import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils

object CaseStream {

  def main (args : Array[String]) : Unit = {
    /*
    val json_string = """
    [{"status": "Open", "category": "CAT3", "sub_category": "SCAT10", "last_modified_timestamp": "2020-09-05 11:12:33", "case_no": "600901", "create_timestamp": "2020-09-05 11:12:33", "created_employee_key": "263886", "call_center_id": "C-123", "product_code": "13164066", "country_cd": "PN", "communication_mode": "Chat"},
    {"status": "Open", "category": "CAT3", "sub_category": "SCAT10", "last_modified_timestamp": "2020-09-05 11:12:33", "case_no": "600902", "create_timestamp": "2020-09-05 11:12:33", "created_employee_key": "406314", "call_center_id": "C-125", "product_code": "10149940", "country_cd": "TF", "communication_mode": "Call"},
    {"status": "Open", "category": "CAT3", "sub_category": "SCAT10", "last_modified_timestamp": "2020-09-05 11:12:33", "case_no": "600903", "create_timestamp": "2020-09-05 11:12:33", "created_employee_key": "401883", "call_center_id": "C-119", "product_code": "2254829", "country_cd": "BO", "communication_mode": "Call"},
    {"status": "Closed", "category": "CAT3", "sub_category": "SCAT10", "last_modified_timestamp": "2020-09-05 11:32:33", "case_no": "600901", "create_timestamp": "2020-09-05 11:12:33", "created_employee_key": "263886", "call_center_id": "C-123", "product_code": "13164066", "country_cd": "PN", "communication_mode": "Chat"},
    {"status":"Closed", "category": "CAT3", "sub_category": "SCAT10", "last_modified_timestamp": "2020-09-05 11:32:33", "case_no": "600902", "create_timestamp": "2020-09-05 11:12:33", "created_employee_key": "406314", "call_center_id": "C-125", "product_code": "10149940", "country_cd": "TF", "communication_mode": "Call"},
    {"status": "Closed", "category": "CAT3", "sub_category": "SCAT10", "last_modified_timestamp": "2020-09-05 11:32:33", "case_no": "600903", "create_timestamp": "2020-09-05 11:12:33", "created_employee_key": "401883", "call_center_id": "C-119", "product_code": "2254829", "country_cd": "BO", "communication_mode": "Call"}]"""
    */

    //Configuring context
    val spark = Utilities.createSparkSession("Stream ingestion of cases to landing area")
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    //Creation of stream from Kafka topic
    val stream = KafkaUtils.createStream(ssc,
      "ip-20-0-21-196.ec2.internal:2181",                       //Bootstrap servers
      "ingestion_group",                                          //group id
      Map("case_735821" -> 1))                                             //consumers per topic

    //Processing of Stream

    stream.foreachRDD( rdd =>{
      if (!rdd.isEmpty()){
        rdd.collect()
          .map(x => x._2)
          .foreach( json_str => {
            val json_array = new JSONParser().parse(json_str).asInstanceOf[JSONArray]
            println(json_array)
        })
      } else {
        println("Empty rdd.... Skipping execution...")
      }
    })

    /*
    val json_array = new JSONParser().parse(json_string).asInstanceOf[JSONArray]
    val iter = json_array.iterator()
    while (iter.hasNext){
      val json_obj = iter.next().asInstanceOf[JSONObject]
      println(CaseRecord(Integer.parseInt(json_obj.get("case_no").toString),
        json_obj.get("create_timestamp").toString(),
        json_obj.get("last_modified_timestamp").toString(),
        Integer.parseInt(json_obj.get("created_employee_key").toString()),
        json_obj.get("call_center_id").toString(),
        json_obj.get("status").toString(),
        json_obj.get("category").toString(),
        json_obj.get("sub_category").toString(),
        json_obj.get("communication_mode").toString(),
        json_obj.get("country_cd").toString(),
        Integer.parseInt(json_obj.get("product_code").toString())))


    }
    val json_arr = JSON.parseFull(json_string)
    JSON.parseFull(json_string) match{
      case Some((m: Map[_, _])) => {
        println(m)
      }
    }*/

  }

}
