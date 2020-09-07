package sources.database

import utils.Utilities
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}

object CaseCountry {

  def main (args : Array[String]) : Unit ={

    val spark = Utilities.createSparkSession("Call center dimension warehouse loading")

    val schema = new StructType(Array(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("alpha_2", StringType, false),
      StructField("alpha_3", StringType, false)))

    // Reading data from hdfs with correct schema
    val country = spark.read
      .format("csv")
      .schema(schema)
      .options(Map("header" -> "true", "sep" -> "\t"))
      .load("/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_case_country_details.txt")

    //Loading data to dimension table
    country.write.mode("append").jdbc(
      Utilities.url,
      "DIM_CASE_COUNTRY",
      Utilities.getDbProps())

  }

}
