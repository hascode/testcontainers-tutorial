package it;


import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ContainerizedKafkaIT {

  @Container
  public KafkaContainer kafkaContainer = new KafkaContainer();

  @Test
  @DisplayName("kafka it should be running")
  void shouldBeRunningKafka() throws Exception {
    assertTrue(kafkaContainer.isRunning());
  }

  @Test
  @DisplayName("should send records over kafka container")
  void shouldSendAndReceiveMessages() throws Exception {
    String servers = kafkaContainer.getBootstrapServers();
    System.out.printf("servers: %s%n", servers);

    Properties props = new Properties();
    props.put("bootstrap.servers", servers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    try (
        Producer<String, String> producer = new KafkaProducer<>(props)) {
      IntStream.range(0,100).forEach(i -> {
        final String msg = String.format("my-message-%d", i);
        producer.send(new ProducerRecord<>("my-topic", msg));
        System.out.println("Sent:" + msg);
      });
    }
  }
}