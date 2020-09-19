/*
  This is a streaming app to calculate the following KPIs on realtime data:
  1. Total numbers of cases open and closed out of the number of cases received
  2. Total number of cases received based on priority and severity

  Usage guideline:
  spark2-submit --class kpi.RealTime \
  --packages mysql:mysql-connector-java:5.1.49,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12,org.apache.clerezza.ext:org.json.simple:0.4
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  target/scala-2.11/customer-retention-strategy_2.11-0.1.jar

 */

package kpi
import sources.stream.CaseStream.stringToArray
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions.{col, explode, split, count, current_timestamp, concat, lit, collect_list}
import org.apache.spark.sql.types.IntegerType
import utils.Utilities

object RealTime {

  def main (args : Array[String]): Unit = {

    //Configuring context
    val spark = Utilities.createSparkSession("KPI calculation on real time data")
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))

    //Creation of stream from Kafka topic
    val stream = KafkaUtils.createStream(ssc,
      "ip-20-0-21-196.ec2.internal:2181",                       //Bootstrap servers
      "case_kpi_calculation_group",                               //group id
      Map("case_735821" -> 2))                                             //consumers per topic

    //Caching lookup data
    val dim_case_category = Utilities.readDB(spark, "DIM_CASE_CATEGORY")
      .select("category_key", "sub_category_key", "priority").cache()

    val dim_case_priority = Utilities.readDB(spark, "DIM_CASE_PRIORITY")
      .select("Priority_key", "priority", "severity").cache()

    //Joining category and priority
    val lookup_data = dim_case_category.join(
      dim_case_priority,
      dim_case_category.col("priority") === dim_case_priority.col("Priority_key"))
      .drop(dim_case_priority.col("Priority_key"))
      .drop(dim_case_category.col("priority"))
      .cache()

    //Unpersist dataframe after join
    dim_case_category.unpersist()
    dim_case_priority.unpersist()

    import spark.implicits._

    //KPI calculation
    val json_arr_stream = stream
      .map( x => x._2.toString)
      .filter( x => x != null)
      .map(json_string =>  stringToArray(json_string))

    json_arr_stream.foreachRDD( rdd => {
      if ( ! rdd.isEmpty()){
        println("...... Start of RDD batch execution .....")

        val df = rdd.toDF("json")
            .select(explode(col("json")).as("json_objs"))
            .filter(col("json_objs").isNotNull)
            .select(split(col("json_objs"), ",").as("arr_spl"))
            .select(col("arr_spl").getItem(0).cast(IntegerType).as("case_no"),
              col("arr_spl").getItem(6).as("category"),
              col("arr_spl").getItem(7).as("sub_category"),
              col("arr_spl").getItem(5).as("status"))

        //Enriching case information with priority and severity of case
        val df_enriched = df.join(lookup_data,
          df.col("category") === lookup_data.col("category_key") && df.col("sub_category") === lookup_data.col("sub_category_key"))

        //Counting total open and close case
        val case_open_closed =  df_enriched
          .groupBy("status").agg(count("case_no").as("total_cases"))
          .select(collect_list(concat(col("status"), lit(":"), col("total_cases"))).as("case_status"))

        //Counting cases priority and severity wise
        val priority_severity = df_enriched
          .groupBy("priority", "severity").agg(count("case_no").as("total_cases"))
          .select(collect_list(concat(col("priority"), lit(","),col("severity"), lit(":"), col("total_cases"))).as("priority_severity"))

        //Joining the above two dataframes
        val load_df = case_open_closed
          .crossJoin(priority_severity)
          .withColumn("ts", current_timestamp)

        load_df.show(false)

        //Load data to cassandra table
        Utilities.loadCassandra(load_df, "crm_kpi_realtime")

        println("....... End of RDD batch execution ......")

      } else {
        println("Empty rdd.... Skipping execution...")
      }
    })
  }

}
