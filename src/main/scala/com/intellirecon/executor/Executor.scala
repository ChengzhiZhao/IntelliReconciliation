package com.intellirecon.executor
import com.intellirecon.datareader.FileReader
import com.intellirecon.receivable.ReceivableHandler

object Executor {


  def main(args: Array[String]):Unit={
    val file = "/home/chengzhizhao/Github/IntelliReconciliation/src/data/Invoices.csv"

    val spark = org.apache.spark.sql.SparkSession.builder()
                  .master("local[*]")
                  .appName("IntelliRecon")
                  .getOrCreate

    val raw_df = new FileReader(spark, file).readCSV()
    val receivableHandler = new ReceivableHandler(spark)
    val matching_df = receivableHandler.assignRowNumber(raw_df).select("ReceivableID", "Invoice Number","Invoice Date","Customer Reference")
    val hash_df = receivableHandler.convertHash(matching_df,Seq("Invoice Number", "Invoice Date", "Customer Reference"))
    hash_df.show
  }
}
