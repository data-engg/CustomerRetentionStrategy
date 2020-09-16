package processing.batch
import utils.Utilities
import org.apache.spark.sql.functions.{col, max, min, to_date}
import org.apache.spark.sql.expressions.Window

object SurveysDailyRealtime {

  def main (args : Array[String]) : Unit = {

    if (args.length > 0){
      if (args(0) != "surveys_daily" && args(0) != "surveys_realtime"){
        println("Valid table names are : surveys_daily and surveys_realtime. Please use either of them")
        System.exit(1)
      }
    } else {
      println("Enter the table name from where data is to be moved")
      System.exit(1)
    }


      val spark = Utilities.createSparkSession("Daily survey landing to main processing")

    //Getting last processed time stamp of surveys_daily/surveys_realtime
    val lastProcessedTS = Utilities.getLastModified(spark, args(0))

    // Reading data from cassandra tables
    val survey_df = Utilities.readCassndraTables(spark, args(0))
      .filter( col("row_insertion_dttm") > lastProcessedTS)
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)


    //calculating earliest timestamp per case
    val case_earliest = survey_df
      .select("case_no","survey_timestamp")
      .groupBy("case_no")
      .agg(min("survey_timestamp").as("earliest_ts"))

    //considering earliest survey
    val surveys_earliest = survey_df
      .join(case_earliest, survey_df.col("case_no") === case_earliest.col("case_no"), "inner")
      .drop(survey_df.col("case_no")).where("survey_timestamp =  earliest_ts").drop("earliest_ts")

    //deduping surverys
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
    val ts_update_df = survey_df
      .select(max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))

    ts_update_df.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(Utilities.getURL, "FACT_SURVEY_LAST_MODIFIED", Utilities.getDbProps())

  }

}
