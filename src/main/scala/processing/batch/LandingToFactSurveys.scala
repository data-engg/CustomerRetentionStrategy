/*
 This spark app incrementally loads survey data from landing tables to warehouse.
 Data from both streaming and batch source is assimilated and then loaded.

 Usage guidelines:
 spark2-submit --class processing.batch.LandingToFactSurveys \
  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.12,mysql:mysql-connector-java:5.1.49
  --conf spark.cassandra.auth.username=..... \
  --conf spark.cassandra.auth.password=..... \
  target/scala-2.11/customer-retention-strategy_2.11-0.1.jar
 */
package processing.batch
import utils.Utilities
import org.apache.spark.sql.functions.{col, max, min, to_date}
import org.apache.spark.sql.expressions.Window

object LandingToFactSurveys {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Daily survey landing to main processing")

    //Getting last processed time stamp of surveys_daily
    var lastProcessedTS = Utilities.getLastModified(spark, "edureka_735821_futurecart_surveys_daily")

    // Reading data from cassandra tables
    val df_daily =  Utilities.readCassndraTables(spark, "edureka_735821_futurecart_surveys_daily")
      .filter( col("row_insertion_dttm") > lastProcessedTS)
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    //Getting last processed time stamp of surveys_realtime
    lastProcessedTS = Utilities.getLastModified(spark, "edureka_735821_futurecart_surveys_realtime")

    // Reading data from cassandra tables
    val df_realtime =  Utilities.readCassndraTables(spark, "edureka_735821_futurecart_surveys_realtime")
      .filter( col("row_insertion_dttm") > lastProcessedTS)
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

    // Accumulating both surveys and realtime data
    val survey_df = df_daily.union(df_realtime)

    //calculating earliest timestamp per case
    val case_earliest = survey_df
      .select("case_no","survey_timestamp")
      .groupBy("case_no")
      .agg(min("survey_timestamp").as("earliest_ts"))

    //considering earliest survey
    val surveys_earliest = survey_df
      .join(case_earliest, survey_df.col("case_no") === case_earliest.col("case_no"), "inner")
      .drop(survey_df.col("case_no")).where("survey_timestamp =  earliest_ts").drop("earliest_ts")

    //deduping surveys
    val windowSpec = Window.partitionBy("case_no")

    val surveys_deduped = surveys_earliest
      .withColumn("max_q1", max("q1").over(windowSpec))
      .withColumn("max_q2", max("q2").over(windowSpec))
      .withColumn("max_q3", max("q3").over(windowSpec))
      .withColumn("max_q5", max("q5").over(windowSpec))
      .where("q1 = max_q1 and q2 = max_q2 and q3 = max_q3 and q5 = max_q5")
      .drop("max_q1","max_q2","max_q3","max_q5")

    //Reading records from table
    val survey_db_df = spark.read
      .jdbc( Utilities.getURL(), "FACT_SURVEY", Utilities.getDbProps())
      .select("case_no")

    // Collecting only the new surveys
    val fresh_survery = surveys_deduped
      .join(survey_db_df, surveys_deduped.col("case_no") === survey_db_df.col("case_no"), "left_outer")
      .filter( survey_db_df.col("case_no").isNull )
      .drop(survey_db_df.col("case_no"))
      .drop("row_insertion_dttm")

    //Load fresh cases to FACT_SURVEY
    Utilities.loadDB(fresh_survery, "FACT_SURVEY")

    //Updating last modified timestamp
    val ts_update_df_daily = df_daily
      .select(max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))

    ts_update_df_daily.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(Utilities.getURL, "EDUREKA_735821_FUTURECART_SURVEYS_DAILY_LAST_MODIFIED", Utilities.getDbProps())

    df_daily.unpersist()

    val ts_update_df_realtime = df_realtime
      .select(max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))

    ts_update_df_realtime.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(Utilities.getURL, "EDUREKA_735821_FUTURECART_SURVEYS_REALTIME_LAST_MODIFIED", Utilities.getDbProps())

    ts_update_df_realtime.unpersist()

  }

}
