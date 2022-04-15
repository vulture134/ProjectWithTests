package test

import domain.Serde._
import domain.{Changes, Key, Stocks}
import io.circe.generic.auto._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

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

  val builder: StreamsBuilder = new StreamsBuilder

  kStreamAggregation(builder)("changes_topic", "total_changes_topic")

  kTableToKTableOuterJoin(builder)("stock_topic", "total_changes_topic", "result_topic", "storeName")

  val topology: Topology = builder.build()

  val application = new KafkaStreams(topology, properties)
  application.start()
  sys.ShutdownHookThread {
    application.close(Duration.ofSeconds(10))

  }
}

