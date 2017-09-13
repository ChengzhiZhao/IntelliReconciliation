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
    val receivableHashColumns = array($"Invoice Number", $"Invoice Date", $"Customer Reference")
    val receivableWithRowNo = receivableHandler.assignReceivableRowNumber(raw_receivable_df)
    val receivable = receivableWithRowNo.withColumn("HashCode", receivableHandler.md5(receivableHashColumns)).alias("receivable")

    val raw_payment_df = new FileReader(spark, paymentFile).readCSV()
    val paymentHandler = new PaymentHandler()
    val paymentHashColumns = array($"Invoice Number", $"Invoice Date", $"Customer Reference")
    val paymentWithRowNo = paymentHandler.assignPaymentRowNumber(raw_payment_df)
    val payment = paymentWithRowNo.withColumn("HashCode", paymentHandler.md5(paymentHashColumns)).alias("payment")

    val r = receivable.join(payment, $"receivable.HashCode" === $"payment.HashCode")
    r.show
  }
}
