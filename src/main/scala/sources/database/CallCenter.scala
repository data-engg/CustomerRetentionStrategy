package sources.database
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utilities
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import sources.database.Calendar.{loadData, readData}

object CallCenter {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Call center dimension warehouse loading")

    //Defining Schema
    val schema = new StructType(Array(
      StructField("call_center_id", StringType, false),
      StructField("call_center_vendor", StringType, false),
      StructField("location", StringType, false),
      StructField("country", StringType, false)
    ))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var callCenter_df : DataFrame = null
    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      callCenter_df = readData(spark, args(0), schema)
    } else {
      callCenter_df = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = callCenter_df, tableName = args(1))
    } else {
      loadData( df = callCenter_df)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_call_center_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "DIM_CALL_CENTER") = {
    Utilities.loadDB(df, tableName)
  }
}
