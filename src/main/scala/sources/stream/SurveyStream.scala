/*
  This spark application polls kafka topic and ingest it into cassandra tables after flattening.
  Usage guidelines:
  spark2-submit --class sources.stream.SurveyStream \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.12,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,org.apache.kafka:kafka-clients:0.8.2.2,org.apache.clerezza.ext:org.json.simple:0.4 \
--conf spark.cassandra.auth.username=..... \
--conf spark.cassandra.auth.password=..... \
target/scala-2.11/customer-retention-strategy_2.11-0.1.jar survey_topic
 */

package sources.stream

import org.apache.spark.sql.functions.{col, explode, split, current_timestamp}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.simple.{JSONArray, JSONObject}
import org.json.simple.parser.JSONParser
import utils.Utilities

import scala.collection.mutable.ArrayBuffer

object SurveyStream {

  def main (args : Array[String]) : Unit = {

    //Configuring context
    val spark = Utilities.createSparkSession("Stream ingestion of survey to landing area")
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))

    //Creation of stream from Kafka topic
    val stream = KafkaUtils.createStream(ssc,
      "ip-20-0-21-196.ec2.internal:2181",                       //Bootstrap servers
      "survey_ingestion_group",                                   //group id
      Map("survey_735821" -> 1))                                           //consumers per topic

    import spark.implicits._
    val stream_print = stream
      .map(x => x._2.toString)
      .map( json_string =>  stringToArray(json_string))
      .foreachRDD( rdd => {
        if ( ! rdd.isEmpty()){

          // Flattening Array to json objects
          val json_df = rdd.toDF("json")
            .select(explode(col("json")).as("json_objs"))

          //Transforming row to columns
          val json_col_df = json_df.filter(col("json_objs").isNotNull)
              .select(split(col("json_objs"), ",").as("arr_spl"))
              .select(col("arr_spl").getItem(0).as("survey_id"),
                col("arr_spl").getItem(1).cast(IntegerType).as("case_no"),
                col("arr_spl").getItem(2).as("survey_timestamp"),
                col("arr_spl").getItem(3).as("q1"),
                col("arr_spl").getItem(4).as("q2"),
                col("arr_spl").getItem(5).as("q3"),
                col("arr_spl").getItem(6).as("q4"),
                col("arr_spl").getItem(7).as("q5")
              ).withColumn("row_insertion_dttm", current_timestamp)

          //Loading rows to landing tables
          Utilities.loadCassandra(json_col_df, "edureka_735821_futurecart_surveys_realtime")

        } else {
          println("Empty rdd.... Skipping execution...")
        }
      })


    try{
      ssc.start()
      ssc.awaitTermination()
    } finally {
      ssc.stop()
    }
  }

  def stringToArray (json_string : String) : Array[String] = {
    val ret_arr = new ArrayBuffer[String]()
    val json_array = new JSONParser().parse(json_string).asInstanceOf[JSONArray]
    val iter = json_array.iterator()
    while (iter.hasNext()){
      val json_obj = iter.next().asInstanceOf[JSONObject]
      ret_arr.append(
        json_obj.get("survey_id").toString() + "," +
          json_obj.get("case_no").toString() + "," +
          json_obj.get("survey_timestamp").toString() + "," +
          json_obj.get("Q1").toString() + "," +
          json_obj.get("Q2").toString() + "," +
          json_obj.get("Q3").toString() + "," +
          json_obj.get("Q4").toString() + "," +
          json_obj.get("Q5").toString())
    }
    ret_arr.toArray[String]
  }
}
