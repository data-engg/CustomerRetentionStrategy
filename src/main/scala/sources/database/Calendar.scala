/*
  This code data from hdfs to dimension tables. Usage guidelines:
  Case 1: with default source and targets
  spark2-submit --class sources.database.Calendar \
  --packages mysql:mysql-connector-java:5.1.49 \
  target/scala-2.11/customer-retention-strategy_2.11-0.1.jar

  Case 2: Change source file location
  spark2-submit --class sources.database.Calendar \
  --packages mysql:mysql-connector-java:5.1.49 \
  target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input hdfs>

  Case 3: Change both source file and target tables
  spark2-submit --class sources.database.Calendar \
  --packages mysql:mysql-connector-java:5.1.49 \
  target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input hdfs> <target tables>
 */

package sources.database

import utils.Utilities
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Calendar {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Calendar dimension warehouse loading")

    //Defining schema
    val schema = new StructType(Array(
      StructField("calendar_date", DateType, false),
      StructField("date_desc", StringType, false),
      StructField("week_day_nbr", IntegerType, false ),
      StructField("week_number", IntegerType, false),
      StructField("week_name", StringType, false),
      StructField("year_week_number", IntegerType, false),
      StructField("month_number", IntegerType, false),
      StructField("month_name", StringType, false),
      StructField("quarter_number", IntegerType, false),
      StructField("quarter_name", StringType, false),
      StructField("half_year_number", IntegerType, false),
      StructField("half_year_name", StringType, false),
      StructField("geo_region_cd", StringType, false)
      ))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var calendar_df : DataFrame = null
    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      calendar_df = readData(spark, args(0), schema)
    } else {
      calendar_df = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = calendar_df, tableName = args(1))
    } else {
      loadData( df = calendar_df)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_calendar_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "EDUREKA_735821_FUTURECART_CALENDAR_DETAILS_LAST_MODIFIED") = {
    Utilities.loadDB(df, tableName)
  }

}
