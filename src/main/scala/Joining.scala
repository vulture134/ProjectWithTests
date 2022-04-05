import java.time.Duration
import java.util.Properties
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._


object Joining extends App {

  object Types {
    type City = String
    case class Stocks (name: String, amount: Int)
    case class Changes (name: String, change: Int)
  }

  object Topics {
    val stockTopic = "stock_topic"
    val changesTopic = "changes_topic"
    val resultTopic = "result_topic"
    val totalChangesTopic = "total_changes_topic"
  }

  import Types._

  implicit def serde [A >: Null : Decoder : Encoder] = {
  val serializer = (a: A) => a.asJson.noSpaces.getBytes()
  val deserializer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
  }
    Serdes.fromFn[A](serializer,deserializer)
  }


  val builder: StreamsBuilder = new StreamsBuilder
  val stockTable: KTable[City, Stocks] = builder.table[City, Stocks](Topics.stockTopic)
  val changesStream: KStream[City, Changes] = builder.stream[City, Changes](Topics.changesTopic)


  val JoinedStream = changesStream.join(stockTable) {(stocks, changes) => Stocks(stocks.name, stocks.change + changes.amount)}
  JoinedStream.to(Topics.resultTopic)

  val totalChanges = changesStream.selectKey((city,changes) => (city,changes.name))
    .groupByKey
    .reduce((value1,value2) => Changes(value2.name, value1.change + value2.change))
  totalChanges.toStream.to(Topics.totalChangesTopic)

  val topology = builder.build()

  val properties: Properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-application")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val application = new KafkaStreams(topology, properties)
  application.start()
    sys.ShutdownHookThread {
    application.close(Duration.ofSeconds(10))
  }
}
