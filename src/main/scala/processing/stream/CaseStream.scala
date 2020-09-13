package processing.stream

import utils.Utilities
import scala.collection.mutable.ArrayBuffer
import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.functions.{col, explode, split, current_timestamp}
import org.apache.spark.sql.types.IntegerType

object CaseStream {

  def main (args : Array[String]) : Unit = {

    //Configuring context
    val spark = Utilities.createSparkSession("Stream ingestion of cases to landing area")
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    //Creation of stream from Kafka topic
    val stream = KafkaUtils.createStream(ssc,
      "ip-20-0-21-196.ec2.internal:2181",                       //Bootstrap servers
      "case_ingestion_group",                                          //group id
      Map("case_735821" -> 1))                                             //consumers per topic

    //Processing of Stream
    import spark.implicits._
    val stream_print = stream
      .map(x => x._2.toString)
      .map( json_string =>  stringToArray(json_string))
      .foreachRDD( rdd =>{
        if ( ! rdd.isEmpty() ) {

          // Flattening Array to json objects
          val json_df = rdd.toDF("json")
            .select(explode(col("json")).as("json_objs"))

          //Transforming row to columns
          val json_col_df = json_df
            .filter(col("json_objs").isNotNull)
            .select(split(col("json_objs"), ",").as("arr_spl"))
            .select(col("arr_spl").getItem(0).cast(IntegerType).as("case_no"),
              col("arr_spl").getItem(4).as("call_center_id"),
              col("arr_spl").getItem(6).as("category"),
              col("arr_spl").getItem(8).as("communication_mode"),
              col("arr_spl").getItem(9).as("country_cd"),
              col("arr_spl").getItem(1).as("create_timestamp"),
              col("arr_spl").getItem(3).cast(IntegerType).as("created_employee_key"),
              col("arr_spl").getItem(2).as("last_modified_timestamp"),
              col("arr_spl").getItem(10).cast(IntegerType).as("product_code"),
              col("arr_spl").getItem(5).as("status"),
              col("arr_spl").getItem(7).as("sub_category"))
            .withColumn("row_insertion_dttm", current_timestamp)

          //Loading rows to landing tables
          Utilities.loadCassandra(json_col_df, "case_realtime" )
      }
        else {
          println("Empty rdd.... Skipping execution...")
      }
    })
  }

  def stringToArray (json_string : String) : Array[String] = {
    val ret_arr = new ArrayBuffer[String]()
    val json_array = new JSONParser().parse(json_string).asInstanceOf[JSONArray]
    val iter = json_array.iterator()
    while (iter.hasNext()){
      val json_obj = iter.next().asInstanceOf[JSONObject]
      ret_arr.append(
      json_obj.get("case_no").toString() + "," +
        json_obj.get("create_timestamp").toString() + "," +
        json_obj.get("last_modified_timestamp").toString() + "," +
        json_obj.get("created_employee_key").toString() + "," +
        json_obj.get("call_center_id").toString() + "," +
        json_obj.get("status").toString() + "," +
        json_obj.get("category").toString() + "," +
        json_obj.get("sub_category").toString() + "," +
      json_obj.get("communication_mode").toString() + "," +
      json_obj.get("country_cd").toString() + "," +
        json_obj.get("product_code").toString())
    }
    ret_arr.toArray[String]
  }
  
}
