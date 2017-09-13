package com.intellirecon.executor
import com.intellirecon.datareader.FileReader
import com.intellirecon.receivable.ReceivableHandler
import com.intellirecon.payment.PaymentHandler
import org.apache.spark.sql.functions._

object Executor {


  def main(args: Array[String]):Unit={

    val receivableFile = "/home/chengzhizhao/Github/IntelliReconciliation/src/data/Invoices.csv"
    val paymentFile = "/home/chengzhizhao/Github/IntelliReconciliation/src/data/Payment.csv"

    val spark = org.apache.spark.sql.SparkSession.builder()
                  .master("local[*]")
                  .appName("IntelliRecon")
                  .getOrCreate
    import spark.implicits._

    val raw_receivable_df = new FileReader(spark, receivableFile).readCSV()
    val receivableHandler = new ReceivableHandler()
    val rcol = Seq("Invoice Number", "Invoice Date","Customer Reference")
    val receivable = receivableHandler.assignReceivableRowNumberWithHashCode(raw_receivable_df, rcol).alias("receivable")
    
    val raw_payment_df = new FileReader(spark, paymentFile).readCSV()
    val paymentHandler = new PaymentHandler()
    val pcol = Seq("Invoice Number", "Invoice Date","Customer Reference")
    val payment = paymentHandler.assignPaymentRowNumberWithHashCode(raw_payment_df, pcol).alias("payment")

    val r = receivable.join(payment, $"receivable.HashCode" === $"payment.HashCode", "outer")
    r.where("payment.HashCode is null").show
  }
}
