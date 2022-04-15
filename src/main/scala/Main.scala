import KafkaStreamsLogic.{KafkaStreamsApp, KafkaStreamsApp2}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.util.Properties

object Main extends App {

  val properties: Properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-application")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val ksApp = new KafkaStreamsApp2

  val application = new KafkaStreams(ksApp.topology, properties)
  application.start()
  sys.ShutdownHookThread {
    application.close(Duration.ofSeconds(10))
  }
}
