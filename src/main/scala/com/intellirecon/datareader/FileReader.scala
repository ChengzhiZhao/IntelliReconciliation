package com.intellirecon.datareader

import org.apache.spark.sql.{DataFrame, SparkSession}

class FileReader(val spark:SparkSession, file: String) {
  def readCSV(): DataFrame = {
    spark.read.option("header", "true").csv(file)
  }
}
