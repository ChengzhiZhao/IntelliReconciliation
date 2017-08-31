package com.intellirecon.receivable
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf, array, lit, typedLit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import java.security.MessageDigest

class ReceivableHandler(val spark: SparkSession) extends Serializable{
  import spark.implicits._

  val md5 = udf((s: Seq[Any]) => {
    val mix = s.map(_.toString).mkString("").getBytes()
    MessageDigest.getInstance("MD5").digest(mix)
  })

  def assignRowNumber(df: DataFrame): DataFrame ={
    df.withColumn("ReceivableID", monotonically_increasing_id + 1)
  }

  def convertHash(df:DataFrame, cols: Seq[String]): DataFrame={
    df.withColumn("HashCode", md5(typedLit(cols)))
  }

}
