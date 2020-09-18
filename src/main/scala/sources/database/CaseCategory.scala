/*
  This code data from hdfs to dimension tables. Usage guidelines:
  Case 1: with default source and targets
  spark2-submit --class sources.database.CaseCategory \
  --packages mysql:mysql-connector-java:5.1.49 \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar

  Case 2: Change source file location
  spark2-submit --class sources.database.CaseCategory \
  --packages mysql:mysql-connector-java:5.1.49 \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input hdfs>

  Case 3: Change both source file and target tables

  spark2-submit --class sources.database.CaseCategory \
  --packages mysql:mysql-connector-java:5.1.49 \
  --target/scala-2.11/customer-retention-strategy_2.11-0.1.jar <input hdfs> <target tables>
 */
package sources.database

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utilities
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CaseCategory {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Case category dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("category_key", StringType, false),
      StructField("sub_category_key", StringType, false),
      StructField("category_description", StringType, false),
      StructField("sub_category_description", StringType, false),
      StructField("priority", StringType, false)
    ))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var priority : DataFrame = null

    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      priority = readData(spark, args(0), schema)
    } else {
      priority = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = priority, tableName = args(1))
    } else {
      loadData( df = priority)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_category_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "DIM_CASE_CATEGORY") = {
    Utilities.loadDB(df, tableName)
  }
}
