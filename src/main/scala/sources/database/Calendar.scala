package sources.database
import utils.Utilities
import org.apache.spark.sql.types.{StructType, StructField, DateType, StringType, IntegerType}

object Calendar {

  def main (args : Array[String]) : Unit = {

    val spark = Utilities.createSparkSession("Calendar dimension warehouse loading")

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


      // Reading data from hdfs with correct schema
      val calendar_df = spark.read
        .format("csv")
        .schema(schema)
        .options(
          Map("header"->"true", "sep"->"\t")
        )
        .load("/bigdatapgp/common_folder/project_futurecart/batchdata/futurecart_calendar_details.txt")

    //Loading data to dimension
    calendar_df.write.mode("overwrite").jdbc(
      Utilities.url,
      "DIM_CALENDAR",
      Utilities.getDbProps()
    )

  }

}
