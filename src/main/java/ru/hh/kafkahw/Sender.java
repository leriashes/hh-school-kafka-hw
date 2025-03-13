package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;
  private static final int MAX_RETRIES = 100;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }

  public void doSomething(String topic, String message) {
    int attempt = 0;
    while (attempt < MAX_RETRIES)
    {
      try {
        producer.send(topic, message);
        break;
      } catch (Exception ignore) {
        attempt++;
        try {
          Thread.sleep(100L * attempt);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
