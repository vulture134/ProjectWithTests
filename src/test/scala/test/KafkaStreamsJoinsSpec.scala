package test
import domain._
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import io.circe.generic.auto._
import java.util
import java.util.Properties

class KafkaStreamsJoinsSpec extends AnyFlatSpec with Matchers {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import scala.jdk.CollectionConverters._

  val inputTopicOne = "input-topic-1"
  val inputTopicTwo = "input-topic-2"
  val outputTopicOne = "output-topic-1"
  val stateStore = "saved-state"

  val stockTopic: util.List[KeyValue[Key, Stocks]] = scala.collection.mutable.Seq[KeyValue[Key, Stocks]](
    (Key("Samara", "hleb"), Stocks(20)),
    (Key("Samara", "moloko"), Stocks(40)),
    (Key("Saratov", "moloko"), Stocks(15)),
    (Key("Togliatti", "sahar"), Stocks(100))
  ).asJava

  val changesTopic: util.List[KeyValue[Key, Changes]] = scala.collection.mutable.Seq[KeyValue[Key, Changes]](
    (Key("Samara", "hleb"), Changes(-19)),
    (Key("Samara", "moloko"), Changes(18)),
    (Key("Saratov", "moloko"), Changes(-5)),
    (Key("Pskov", "sol"), Changes(25))
  ).asJava

  val config = new Properties()
  config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testing2")
  config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testing:1235")


  // -------  KTable to KTable Joins ------------ //
  "KTable to KTable Outer join" should "modify the amount value by the change value of the corresponding key" in {

    val driver = new TopologyTestDriver(
      KafkaStreamsJoins.kTableToKTableOuterJoin(inputTopicOne, inputTopicTwo, outputTopicOne, stateStore), config)

    val inputTopic1 = driver.createInputTopic(inputTopicOne, Serde.serde[Key].serializer, Serde.serde[Stocks].serializer)
    val inputTopic2 = driver.createInputTopic(inputTopicTwo, Serde.serde[Key].serializer, Serde.serde[Changes].serializer)

    inputTopic1.pipeKeyValueList(stockTopic)
    inputTopic2.pipeKeyValueList(changesTopic)

    val testOutputTopic = driver.createOutputTopic(outputTopicOne, Serde.serde[Key].deserializer, Serde.serde[Stocks].deserializer)
    val results = testOutputTopic.readKeyValuesToMap()
    assert(results.get(Key("Samara", "hleb")) == Stocks(1))
    assert(results.get(Key("Samara", "moloko")) == Stocks(58))
    assert(results.get(Key("Saratov", "moloko")) == Stocks(10))
    assert(results.get(Key("Togliatti", "sahar")) == Stocks(100))
    assert(results.get(Key("Pskov", "sol")) == Stocks(25))

    driver.close()
  }
}
