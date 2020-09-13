package processing.batch

import utils.Utilities
import org.apache.spark.sql.functions.{col, date_format}
object CaseDaily {

  def main (args : Array[String]) : Unit = {

    if (args.length > 0){
      if (args(0) != "case_realtime" && args(0) != "case_daily"){
        println("Valid table names are : case_realtime and case_daily. Please use either of them")
        System.exit(1)
      }
    } else {
      println("Enter the table name from where data is to be moved")
      System.exit(1)
    }

    val spark = Utilities.createSparkSession("Daily case landing to main processing")

    //Processing date
    val process_date = "2020-04-20"

    // Reading data from cassandra tables

    val case_df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "edureka_735821", "table" -> args(0)))
      .load()
      .drop("row_insertion_dttm")
      .filter( date_format(col("create_timestamp"), "yyyy-MM-dd") === process_date)

    Utilities.loadDB(case_df, "CASE_DAILY")
  }

}
