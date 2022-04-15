package test

import KafkaStreamsLogic.KafkaStreamsJoins
import domain._
import io.circe.generic.auto._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

import java.util
import java.util.Properties

class KafkaStreamsAggregationSpec extends AnyFlatSpec with Matchers {

  import org.apache.kafka.streams.scala.ImplicitConversions._

  import scala.jdk.CollectionConverters._

  val inputTopic = "input-topic"
  val outputTopic = "output-topic"

  val changesTopicA: util.List[KeyValue[Key, Changes]] = scala.collection.mutable.Seq[KeyValue[Key, Changes]](
    (Key("Volsk", "vodka"), Changes(20)),
    (Key("Volsk", "vodka"), Changes(-15)),
    (Key("Syzran", "pivo"), Changes(-10)),
    (Key("Syzran", "vino"), Changes(-19))
  ).asJava

  val config = new Properties()
  config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testing")
  config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testing:1234")

  // -------  Aggregation of kStream data ------------ //
  "Aggregation" should "sum all the changes' values of the corresponding key" in {

    val builder = new StreamsBuilder()

    KafkaStreamsJoins.kStreamAggregation(builder)(inputTopic, outputTopic)

    val driver = new TopologyTestDriver(builder.build(), config)

    val inputTopicA = driver.createInputTopic(inputTopic, Serde.serde[Key].serializer, Serde.serde[Changes].serializer)

    inputTopicA.pipeKeyValueList(changesTopicA)

    val testOutputTopic = driver.createOutputTopic(outputTopic, Serde.serde[Key].deserializer, Serde.serde[Changes].deserializer)
    val results = testOutputTopic.readKeyValuesToMap()
    assert(results.get(Key("Volsk", "vodka")) == Changes(5))
    assert(results.get(Key("Syzran", "pivo")) == Changes(-10))
    assert(results.get(Key("Syzran", "vino")) == Changes(-19))

    driver.close()
  }
}
