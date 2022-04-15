package KafkaStreamsLogic

import KafkaStreamsLogic.KafkaStreamsJoins.{kStreamAggregation, kTableToKTableOuterJoin, properties}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder


class KafkaStreamsApp2 {
  val builder: StreamsBuilder = new StreamsBuilder

  kStreamAggregation(builder)("changes_topic", "total_changes_topic")

  kTableToKTableOuterJoin(builder)("stock_topic", "total_changes_topic", "result_topic", "storeName")

  val topology: Topology = builder.build()
}
