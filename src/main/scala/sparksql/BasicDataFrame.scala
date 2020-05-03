package main.scala.sparksql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object BasicDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic data frame")
      .master("local[*]")
      .getOrCreate()

    /**
     * RDD[Row]
     * Schema
     * to create an RDD and convert it to an RDD[Row]
     */
    val numRDD = spark.sparkContext.parallelize(Array(10,11,12,13,14,15,16))
    val numRowRDD = numRDD.map(item => Row(item))
    val numSchema = StructType(StructField("number", IntegerType, true) ::  Nil)

    val numDF = spark.createDataFrame(numRowRDD, numSchema)
    numDF.printSchema()
    numDF.show()
  }
}