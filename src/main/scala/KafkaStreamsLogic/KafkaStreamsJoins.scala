package KafkaStreamsLogic

import domain.{Changes, Key, Stocks}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.{StreamsConfig}
import domain.Serde._
import io.circe.generic.auto._
import org.apache.kafka.streams.scala.ImplicitConversions._



import java.time.Duration
import java.util.Properties

object KafkaStreamsJoins {

  val properties: Properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-application")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  def kStreamAggregation(builder: StreamsBuilder)(inputTopic: String, outputTopicA: String): Unit = {

    val changesStream: KStream[Key, Changes] = builder.stream[Key, Changes](inputTopic)
    val totalChanges: KTable[Key, Changes] = changesStream
      .groupByKey
      .reduce((value1, value2) => Changes(value1.change + value2.change))
    totalChanges.toStream.to(outputTopicA)

  }

  def kTableToKTableOuterJoin(builder: StreamsBuilder)(inputTopic1: String, inputTopic2: String, outputTopic: String, storeName: String): Unit = {

    val stockTable: KTable[Key, Stocks] = builder.table[Key, Stocks](inputTopic1)
    val changesTable: KTable[Key, Changes] = builder.table[Key, Changes](inputTopic2)

    val mat: Materialized[Key, Stocks, ByteArrayKeyValueStore] = Materialized.as(storeName)

    changesTable.outerJoin(stockTable, mat) {
      case (changes, null) => Stocks(changes.change)
      case (null, stocks) => Stocks(stocks.amount)
      case (changes, stocks) => Stocks(changes.change + stocks.amount)
    }.toStream.to(outputTopic)

  }

}
