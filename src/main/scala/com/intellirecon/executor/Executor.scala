package com.intellirecon.executor
import com.intellirecon.datareader.FileReader
import com.intellirecon.receivable.ReceivableHandler
import com.intellirecon.payment.PaymentHandler
import com.intellirecon.rule.RuleReader
import com.intellirecon.matcher.Matcher
object Executor {


  def main(args: Array[String]):Unit={

    val receivableFile = "/home/chengzhizhao/Github/IntelliReconciliation/src/data/Invoices.csv"
    val paymentFile = "/home/chengzhizhao/Github/IntelliReconciliation/src/data/Payment.csv"

    val spark = org.apache.spark.sql.SparkSession.builder()
                  .master("local[*]")
                  .appName("IntelliRecon")
                  .getOrCreate

    val ruleReader = new RuleReader()
    val rules = ruleReader.read("/home/chengzhizhao/Github/IntelliReconciliation/src/main/scala/com/intellirecon/rule/rules.yaml")

    val raw_receivable_df = new FileReader(spark, receivableFile).readCSV()
    val receivableHandler = new ReceivableHandler()

    val raw_payment_df = new FileReader(spark, paymentFile).readCSV()
    val paymentHandler = new PaymentHandler()

    val matcher = new Matcher(spark)
    val r = matcher.matchOnRules(rules.sortBy(r=>r.sequence), raw_receivable_df, receivableHandler, raw_payment_df, paymentHandler)
    r.show()
    }
}
