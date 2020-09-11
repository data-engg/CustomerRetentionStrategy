package sources.database

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sources.database.Product.{loadData, readData}
import utils.Utilities

object Survey {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Survey lookup warehouse loading")

    val schema = new StructType(Array(
      StructField("question_id", StringType, false),
      StructField("question_desc", StringType, false),
      StructField("response_type", StringType, false),
      StructField("response_range", StringType, false),
      StructField("negative_response_range", StringType, false),
      StructField("neutral_response_range", StringType, false),
      StructField("positive_response_range", StringType, false)))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var survey : DataFrame = null
    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      survey = readData(spark, args(0), schema)
    } else {
      survey = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = survey, tableName = args(1))
    } else {
      loadData( df = survey)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_survey_question_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "SURVEY_LKP") = {
    Utilities.loadDB(df, tableName)
  }

}
