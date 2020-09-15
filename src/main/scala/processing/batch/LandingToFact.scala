package processing.batch

import utils.Utilities
import org.apache.spark.sql.functions.{col, max, to_date}

object LandingToFact {

  def main (args : Array[String]) : Unit = {

    val validTables = Array("case_daily", "case_realtime", "surveys_daily", "surveys_realtime ")

    if (args.length < 1){
      println("Enter the name of cassandra table.... Aborting job execution")
      System.exit(1)
    } else {
      if ( ! validTables.contains(args(0).toLowerCase)){
        println("Enter valid table names.... Valid table names are 'CASE_REALTIME' and 'CASE_DAILY' ")
        System.exit(1)
      }
    }

    val inputTableName : String = args(0).toLowerCase()

    val spark = Utilities.createSparkSession("Incremental loading from landing tables(cassandra) to fact tables(mysql)")

    //Getting processed time stamp of case_daily/case_realtime
    val lastProcessedTS = Utilities.getLastModified(spark, inputTableName)

    //Taking records which are inserted after last processed time
    val df = Utilities.readCassndraTables(spark, inputTableName).filter( col("row_insertion_dttm") > lastProcessedTS)

    //persistsing the dataframe. Done to freeze the dataframe through out the life of code execution
    df.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    //Load incremental data to FACT_CASE/FACT_SURVEY
    if (inputTableName.contains("case")) {
      Utilities.loadDB(df.drop("row_insertion_dttm"), "FACT_CASE")
    }

    if (inputTableName.contains("case")) {
      Utilities.loadDB(df.drop("row_insertion_dttm"), "FACT_SURVEY")
    }

    //Updating last modified timestamp
    val ts_update_df = df
      .select(max("row_insertion_dttm").as("LAST_MODIFIED_TS"))
      .select(to_date(col("LAST_MODIFIED_TS")).as("LAST_MODIFIED_DATE"), col("LAST_MODIFIED_TS"))

    ts_update_df.write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .jdbc(Utilities.getURL, inputTableName.toUpperCase + "_LAST_MODIFIED", Utilities.getDbProps())

    df.unpersist()

  }
}
