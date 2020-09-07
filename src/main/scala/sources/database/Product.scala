package sources.database

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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

    // Reading data from hdfs with correct schema
    val product = spark.read
      .format("csv")
      .schema(schema)
      .options(Map("header" -> "true", "sep" -> "\t"))
      .load("/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_product_details.txt")

    //Loading data to dimension table
    product.write.mode("append").jdbc(
      Utilities.url,
      "DIM_PRODUCT",
      Utilities.getDbProps())
  }

}
