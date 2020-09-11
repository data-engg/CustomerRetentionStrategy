package sources.database

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sources.database.Employee.{loadData, readData}
import utils.Utilities

object Product {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Product dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("product_id", IntegerType, false),
      StructField("department", StringType, false),
      StructField("brand", StringType, false),
      StructField("commodity_desc", StringType, false),
      StructField("sub_commodity_desc", StringType, false)))

    //Defining a file system
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var product : DataFrame = null

    // Reading data from hdfs with correct schema
    if (args.length == 1 && fs.exists(new Path(args(0)))){
      product = readData(spark, args(0), schema)
    } else {
      product = readData(session = spark, schm = schema)
    }

    //Loading data to dimension
    if (args.length == 2){
      loadData( df = product, tableName = args(1))
    } else {
      loadData( df = product)
    }
  }

  /*Function to read data from hdfs.
  This makes it possible to read from default location as well as change input location just by adding an argument at run time*/
  def readData( session : SparkSession,
                hdfsLoc : String = "/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_product_details.txt",
                schm : StructType) : DataFrame = {

    Utilities.readDimData(session, hdfsLoc, schm)

  }
  /*Function to load data from to RDBMS.
    This makes it possible to load to default table as well as change target table just by adding an argument at run time*/
  def loadData( df : DataFrame, tableName : String = "DIM_PRODUCT") = {
    Utilities.loadDB(df, tableName)
  }

}
