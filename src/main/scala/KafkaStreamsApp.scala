import domain.Serde._
import domain.{Changes, Key, Stocks, Topics}
import io.circe.generic.auto._
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}

class KafkaStreamsApp {
  val builder: StreamsBuilder = new StreamsBuilder
  val stockTable: KTable[Key, Stocks] = builder.table[Key, Stocks](Topics.stockTopic)
  val changesStream: KStream[Key, Changes] = builder.stream[Key, Changes](Topics.changesTopic)

  val totalChanges: KTable[Key, Changes] = changesStream
    .groupByKey
    .reduce((value1, value2) => Changes(value1.change + value2.change))
  totalChanges.toStream.to(Topics.totalChangesTopic)

  val joinedStream: KTable[Key, Stocks] = totalChanges.outerJoin(stockTable) {
    case (changes, null) =>  Stocks(changes.change)
    case (null, stocks) =>  Stocks(stocks.amount)
    case (changes, stocks) => Stocks(changes.change + stocks.amount)
  }
  joinedStream.toStream.to(Topics.resultTopic)

  val topology: Topology = builder.build()
}
