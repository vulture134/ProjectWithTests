package test

import domain.Serde._
import domain.{Changes, Key, Stocks}
import io.circe.generic.auto._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.{StreamsConfig, Topology}

import java.util.Properties

object KafkaStreamsJoins {

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-examples")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  def kStreamAggregation(inputTopic: String, outputTopicA: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder
    val changesStream: KStream[Key, Changes] = builder.stream[Key, Changes](inputTopic)
    val totalChanges: KTable[Key, Changes] = changesStream
      .groupByKey
      .reduce((value1, value2) => Changes(value1.change + value2.change))
    totalChanges.toStream.to(outputTopicA)

    builder.build()
  }

  def kTableToKTableOuterJoin(inputTopic1: String, inputTopic2: String, outputTopic: String, storeName: String): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder
    val stockTable: KTable[Key, Stocks] = builder.table[Key, Stocks](inputTopic1)
    val changesTable: KTable[Key, Changes] = builder.table[Key, Changes](inputTopic2)

    val mat: Materialized[Key, Stocks, ByteArrayKeyValueStore] = Materialized.as(storeName)

    changesTable.outerJoin(stockTable, mat) {
      case (changes, null) => Stocks(changes.change)
      case (null, stocks) => Stocks(stocks.amount)
      case (changes, stocks) => Stocks(changes.change + stocks.amount)
    }.toStream.to(outputTopic)

    builder.build()
  }
}
