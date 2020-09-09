package processing

import utils.Utilities
import org.apache.spark.sql.functions.{col, date_format}
object CaseDaily {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Daily case landing to main processing")

    //Processing date
    val process_date = "2020-04-20"

    // Reading data from cassandra tables

    val case_df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table" -> "case_daily"))
      .load()
      .filter( date_format(col("create_timestamp"), "yyyy-MM-dd") === process_date)

    Utilities.loadDB(case_df, "CASE_DAILY")
  }

}
