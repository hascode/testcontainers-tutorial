package it;


import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
  @DisplayName("kafka server should be running")
  void shouldBeRunningKafka() throws Exception {
    assertTrue(kafkaContainer.isRunning());
  }

  @Test
  @DisplayName("should send and receive records over kafka")
  void shouldSendAndReceiveMessages() throws Exception {
    var servers = kafkaContainer.getBootstrapServers();
    System.out.printf("servers: %s%n", servers);

    var props = new Properties();
    props.put("bootstrap.servers", servers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("group.id", "group-1");

    var counter = new AtomicInteger(0);

    new Thread(() -> {
      KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
      kafkaConsumer.subscribe(Arrays.asList("my-topic"));
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(20);
        records.forEach(record -> {
          System.out
              .printf("%d # offset: %d, value = %s%n", counter.incrementAndGet(), record.offset(),
                  record.value());
        });
      }

    }).start();

    try (
        Producer<String, String> producer = new KafkaProducer<>(props)) {
      IntStream.range(0, 100).forEach(i -> {
        var msg = String.format("my-message-%d", i);
        producer.send(new ProducerRecord<>("my-topic", msg));
        System.out.println("Sent:" + msg);
      });
    }
  }
}