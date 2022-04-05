import domain.Serde._
import domain.{Changes, Key, Stocks}
import io.circe.generic.auto._
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

class KafkaStreamsApp(stockTopic: String, changeTopic: String, changeOutputTopic: String, stockOutputTopic: String) {
  val builder: StreamsBuilder = new StreamsBuilder
  val stockTable: KTable[Key, Stocks] = builder.table[Key, Stocks](stockTopic)
  val changesStream: KStream[Key, Changes] = builder.stream[Key, Changes](changeTopic)


  val joinedStream: KStream[Key, Stocks] = changesStream.join(stockTable) {
    (stocks, changes) => Stocks(stocks.change + changes.amount)
  }
  joinedStream.to(stockOutputTopic)

  val totalChanges: KTable[Key, Changes] = changesStream
    .groupByKey
    .reduce((value1,value2) => Changes(value1.change + value2.change))
  totalChanges.toStream.to(changeOutputTopic)

  val topology: Topology = builder.build()
}
