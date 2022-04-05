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
    case class Str1 (name: String, amount: Int)
    case class Str2 (name: String, change: Int)
  }

  object Topics {
    val stockTopic = "topic4"
    val changes = "topic5"
    val result = "topic6"
    val totalChanges = "topic7"
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
  val Stream1: KTable[City, Str1] = builder.table[City, Str1](Topics.stockTopic)
  val Stream2: KStream[City, Str2] = builder.stream[City, Str2](Topics.changes)


  val Joined1 = Stream2.join(Stream1) {  (first, second) => Str1(first.name, first.change + second.amount) }
  Joined1.to(Topics.result)

  val AllChanges = Stream2.groupByKey
    .reduce((value1,value2) => Str2(value2.name, value1.change + value2.change))
  AllChanges.toStream.to(Topics.totalChanges)

  val topology = builder.build()

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val app = new KafkaStreams(topology, props)
  app.start()
    sys.ShutdownHookThread {
    app.close(Duration.ofSeconds(10))
  }
}
