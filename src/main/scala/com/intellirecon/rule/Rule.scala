package com.intellirecon.rule
import java.io.{File, FileInputStream}
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

@JsonCreator
case class Rules(@JsonProperty("Rules") rules: Seq[Rule]) {
}

@JsonCreator
case class Rule(@JsonProperty("ruleName") ruleName: String,
           @JsonProperty("receivableColumns") receivableColumns: Seq[String],
           @JsonProperty("paymentColumns") paymentColumns: Seq[String],
           @JsonProperty("sequence") sequence: Int) {
}

class RuleReader(){
  def read(filePath: String): Seq[Rule] = {
    val newConfiguration = new File(filePath)
    val is = new FileInputStream(newConfiguration)

    val mapper = new ObjectMapper(new YAMLFactory)
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    val rules = mapper.readValue(is, classOf[Rules])
    rules.rules
  }
}