package processing

import utils.Utilities
import org.apache.spark.sql.functions.{col, date_format, min, max}
import org.apache.spark.sql.expressions.Window
object SurveysDaily {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Daily survey landing to main processing")

    //Processing date
    val process_date = "2020-04-20"

    // Reading data from cassandra tables
    val survey_df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table" -> "surveys_daily"))
      .load()
      .drop("row_insertion_dttm")
      .filter( date_format(col("survey_timestamp"), "yyyy-MM-dd") === process_date)

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
      .jdbc( Utilities.url, "FACT_SURVEY", Utilities.getDbProps())
      .select("case_no")

    // Collecting only the new surveys
    val fresh_survery = surveys_deduped
      .join(survey_db_df, surveys_deduped.col("case_no") === survey_db_df.col("case_no"), "left_outer")
      .filter( survey_db_df.col("case_no").isNull )
      .drop(survey_db_df.col("case_no"))

    Utilities.loadDB(fresh_survery, "FACT_SURVEY")

  }

}
