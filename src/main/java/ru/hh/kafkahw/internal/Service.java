package ru.hh.kafkahw.internal;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Service {

  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();
  private final Random random = new Random();
  private final static Logger LOGGER = LoggerFactory.getLogger(Service.class);

  public void handle(String topic, String message) {
    boolean handled = false;
    while (!handled)
    {
      try {
        if (random.nextInt(100) < 10) {
          throw new RuntimeException();
        }
        counters.computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
            .computeIfAbsent(message, key -> new AtomicInteger(0)).incrementAndGet();
        handled = true;
        if (random.nextInt(100) < 2) {
          throw new RuntimeException();
        }
      } catch (Exception e) {
        LOGGER.error("Failed to handle message, topic: {}, message: {}", topic, message, e);
      }
    }
  }

  public int count(String topic, String message) {
    return counters.getOrDefault(topic, new ConcurrentHashMap<>()).getOrDefault(message, new AtomicInteger(0)).get();
  }
}
