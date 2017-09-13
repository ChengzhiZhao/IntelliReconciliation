package com.intellirecon.payment

import org.apache.spark.sql.{Column, DataFrame}
import util.Handler

class PaymentHandler() extends Serializable with Handler {

  def assignPaymentRowNumber(raw_df: DataFrame): DataFrame ={
    val matching_df = assignRowNumber("PaymentID",raw_df)
    matching_df
  }
}
