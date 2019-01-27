package it;


import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void kafkaShouldBeRunning() throws Exception {
    assertTrue(kafkaContainer.isRunning());
  }
}