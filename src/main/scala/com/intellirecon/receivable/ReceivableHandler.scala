package com.intellirecon.receivable
import org.apache.spark.sql.{DataFrame}
import util.Handler

class ReceivableHandler() extends Serializable with Handler{

  def assignReceivableRowNumber(raw_df: DataFrame): DataFrame ={
    val matching_df = assignRowNumber("ReceivableID", raw_df)
    matching_df
  }

}
