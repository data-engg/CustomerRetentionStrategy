package sources.database

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utilities
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sources.database.CallCenter.{loadData, readData}

object CaseCountry {

  def main (args : Array[String]) : Unit ={

    val spark = Utilities.createSparkSession("Call center dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("alpha_2", StringType, false),
      StructField("alpha_3", StringType, false)))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var country : DataFrame = null

    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      country = readData(spark, args(0), schema)
    } else {
      country = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = country, tableName = args(1))
    } else {
      loadData( df = country)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_country_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "DIM_CASE_COUNTRY") = {
    Utilities.loadDB(df, tableName)
  }
}
