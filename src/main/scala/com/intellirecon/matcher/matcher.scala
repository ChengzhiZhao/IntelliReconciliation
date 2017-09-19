package com.intellirecon.matcher

import com.intellirecon.payment.PaymentHandler
import com.intellirecon.receivable.ReceivableHandler
import com.intellirecon.rule.Rule
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

import scala.collection.Seq

class Matcher (spark: SparkSession) {
    def matchOnRules(rules: Seq[Rule],
                   r_df: DataFrame, r_handler: ReceivableHandler,
                   p_df:DataFrame, p_handler: PaymentHandler): DataFrame ={
    import spark.implicits._
    def loop(rules: Seq[Rule],
             r_df: DataFrame, r_handler: ReceivableHandler,
             p_df:DataFrame, p_handler: PaymentHandler):DataFrame={
      rules match {
        case Nil => r_df.withColumn("RuleName", lit(null))
        case head+:tail => {
          val rule = head
          val r_rules = tail

          println(s"Matching on ${rule.ruleName}")

          val rcol = rule.receivableColumns
          val receivable = r_handler.assignReceivableRowNumberWithHashCode(r_df, rcol).alias("receivable")

          val pcol = rule.paymentColumns
          val payment = p_handler.assignPaymentRowNumberWithHashCode(p_df, pcol).alias("payment")

          val m = receivable.join(payment, $"receivable.HashCode" === $"payment.HashCode", "outer")
            .withColumn("RuleName", when(col("payment.HashCode").isNotNull, lit(rule.ruleName)))
          val unmatchedReceivable = m.where("payment.HashCode is null").select("receivable.*")
          val matchedReceivable = m.where("payment.HashCode is not null").select("receivable.*","RuleName")
          val restResult = loop(r_rules, unmatchedReceivable, r_handler, p_df, p_handler)
          matchedReceivable.union(restResult).filter(col("Invoice Number").isNotNull)
        }
      }
    }
    loop(rules, r_df, r_handler, p_df, p_handler)
  }

}
