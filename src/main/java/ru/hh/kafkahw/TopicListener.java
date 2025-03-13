package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final Set<String> cacheTopic1 = ConcurrentHashMap.newKeySet();
  private final Set<String> cacheTopic3 = ConcurrentHashMap.newKeySet();

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    ack.acknowledge();
    if (!cacheTopic1.contains(consumerRecord.value())) {
      cacheTopic1.add(consumerRecord.value());
      LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
      service.handle("topic1", consumerRecord.value());
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic2", consumerRecord.value());
    ack.acknowledge();
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    if (!cacheTopic3.contains(consumerRecord.value())) {
      try {
        LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
        service.handle("topic3", consumerRecord.value());
        cacheTopic3.add(consumerRecord.value());
      } catch (RuntimeException e) {
        LOGGER.error("Failed to handle message, topic: {}, payload: {}", consumerRecord.topic(), consumerRecord.value(), e);
        throw e;
      }
    }
    ack.acknowledge();
  }
}
