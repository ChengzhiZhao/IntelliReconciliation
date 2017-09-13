package util

import java.security.MessageDigest

import org.apache.spark.sql.{Column, Row, DataFrame}
import org.apache.spark.sql.functions._

trait Handler extends Serializable {
  def md5 = udf((s: Seq[Any]) => {
    val mix = s.map(_.toString).mkString("").getBytes("UTF-8")
    MessageDigest.getInstance("MD5").digest(mix)
  })

  def assignRowNumber(colName:String, df: DataFrame): DataFrame ={
    df.withColumn(colName, monotonically_increasing_id + 1)
  }

  def generateHashColumn(df: DataFrame, cols: Seq[String]): DataFrame={
    df.withColumn("HashCode", md5(array(cols.head, cols.tail:_*)))
  }
}
