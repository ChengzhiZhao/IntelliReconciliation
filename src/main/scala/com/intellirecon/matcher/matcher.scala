package com.intellirecon.matcher

import com.intellirecon.payment.PaymentHandler
import com.intellirecon.receivable.ReceivableHandler
import com.intellirecon.rule.Rule
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

import scala.collection.Seq

class Matcher (spark: SparkSession) {
    import spark.implicits._

    def matchOnReceivable(rules: Seq[Rule],
                          r_df: DataFrame, r_handler: ReceivableHandler,
                          p_df:DataFrame, p_handler: PaymentHandler): DataFrame={
      matchOnRules(rules, r_df,r_handler,p_df,p_handler,reserved = "receivable")
    }

    def matchOnPayment(rules: Seq[Rule],
                          r_df: DataFrame, r_handler: ReceivableHandler,
                          p_df:DataFrame, p_handler: PaymentHandler): DataFrame={
      matchOnRules(rules, r_df,r_handler,p_df,p_handler,reserved = "payment")
    }


    def matchOnRules(rules: Seq[Rule],
                   r_df: DataFrame, r_handler: ReceivableHandler,
                   p_df:DataFrame, p_handler: PaymentHandler, reserved: String): DataFrame ={

      def loop(rules: Seq[Rule],
               r_df: DataFrame, r_handler: ReceivableHandler,
               p_df:DataFrame, p_handler: PaymentHandler):DataFrame={
        rules match {
          case Nil => if (reserved=="receivable") r_df.withColumn("RuleName", lit(null)) else p_df.withColumn("RuleName", lit(null))
          case head+:tail => {
            val rule = head
            val r_rules = tail

            println(s"Reserved ${reserved}, Matching on ${rule.ruleName}")

            val rcol = rule.receivableColumns
            val receivable = r_handler.assignReceivableRowNumberWithHashCode(r_df, rcol).alias("receivable")

            val pcol = rule.paymentColumns
            val payment = p_handler.assignPaymentRowNumberWithHashCode(p_df, pcol).alias("payment")


            reserved match {
              case "receivable" =>
                val m = receivable.join(payment, $"receivable.HashCode" === $"payment.HashCode", "outer")
                  .withColumn("RuleName", when(col("payment.HashCode").isNotNull, lit(rule.ruleName)))
                val unmatchedReceivable = m.where("payment.HashCode is null").select("receivable.*")
                val matchedReceivable = m.where("payment.HashCode is not null").select("receivable.*","RuleName")
                val restResult = loop(r_rules, unmatchedReceivable, r_handler, p_df, p_handler)
                matchedReceivable.union(restResult).filter(col("Invoice Number").isNotNull)
              case "payment" =>
                val m = payment.join(receivable, $"payment.HashCode" === $"receivable.HashCode" , "outer")
                  .withColumn("RuleName", when(col("receivable.HashCode").isNotNull, lit(rule.ruleName)))
                val unmatchedPayment = m.where("receivable.HashCode is null").select("payment.*")
                val matchedPayment = m.where("receivable.HashCode is not null").select("payment.*","RuleName")
                val restResult = loop(r_rules, r_df, r_handler, unmatchedPayment, p_handler)
                matchedPayment.union(restResult).filter(col("Invoice Number").isNotNull)
            }



          }
        }
      }
    loop(rules, r_df, r_handler, p_df, p_handler)
  }

}
