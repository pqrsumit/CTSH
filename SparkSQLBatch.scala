package sql

import org.apache.spark.sql.SparkSession

object SparkSQLBatch {

  def main(args: Array[String]): Unit = {
    println("Scala is working")

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Spark SQL Batch Demo")
      .getOrCreate()

    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions",2)
    spark.sparkContext.setLogLevel("WARN")


    val AircraftRawData = spark.read
        .option("sep","\t")
      .option("header","true")
        .csv("F:\\Spark-Data\\data-master\\CTS-Demo\\Scripts-Commands\\CTSH\\Aircrafts.txt")
    AircraftRawData.printSchema()
    AircraftRawData.show()

//    AircraftRawData.write
//        .mode("overwrite")
//      .orc("F:\\Spark-Data\\data-master\\CTS-Demo\\Scripts-Commands\\CTSH\\Output\\Aircrafts.orc")
    AircraftRawData.write
      .mode("overwrite")
          .format("json")
          .save("F:\\Spark-Data\\data-master\\CTS-Demo\\Scripts-Commands\\CTSH\\Output\\Aircrafts.json")

    val AircraftJSON = spark.read
      .json("F:\\Spark-Data\\data-master\\CTS-Demo\\Scripts-Commands\\CTSH\\Output\\Aircrafts.json\\")

    AircraftJSON.show()
    AircraftJSON.write
      .mode("overwrite")
      .parquet("F:\\Spark-Data\\data-master\\CTS-Demo\\Scripts-Commands\\CTSH\\Output\\Aircraft.Parquet")

    AircraftJSON.write.format("avro")
      .mode("overwrite")
      .save("F:\\Spark-Data\\data-master\\CTS-Demo\\Scripts-Commands\\CTSH\\Output\\Aircraft.Avro")



  }

}
