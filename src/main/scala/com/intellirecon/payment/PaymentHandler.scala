package com.intellirecon.payment

import org.apache.spark.sql.{Column, DataFrame}
import util.Handler

class PaymentHandler() extends Serializable with Handler {

  def assignPaymentRowNumberWithHashCode(raw_df: DataFrame, cols: Seq[String]): DataFrame ={
    val matching_df = assignRowNumber("PaymentID",raw_df)
    val hs = generateHashColumn(matching_df, cols)
    hs
  }
}
