package com.intellirecon.receivable
import org.apache.spark.sql.{DataFrame}
import util.Handler

class ReceivableHandler() extends Serializable with Handler{

  def assignReceivableRowNumberWithHashCode(raw_df: DataFrame, cols: Seq[String]): DataFrame ={
    val matching_df = assignRowNumber("ReceivableID",raw_df)
    val hs = generateHashColumn(matching_df, cols)
    hs
  }

}
