package rrsesino.kafka.productor.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rrsesino.kafka.productor.AsyncProducer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FetchConsumer {
  private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "FetchConsumer");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "ClientId_FetchConsumer"); // logs, quotas: it identifies the client

    // Controlling the minimum:
    config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
    config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1024);

    // Contorlling the maximum:
    config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024);

    // Limiting by amount of messages
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // we take messages by 10


    config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000"); // milisecs
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "9000"); // 3 times the previous value

    // remaining timeouts
    config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "9000");

    // waiting time per request
    config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "9000");





    KafkaConsumer<String, String> consumer = null;

    List<String> topics = new ArrayList<>();
    topics.add("basic-producer");

    try {
      consumer = new KafkaConsumer<String, String>(config);
      consumer.subscribe(topics);

      while(true) {
        log.info("Log before poll");
        ConsumerRecords<String, String> rs = consumer.poll(Duration.ofMillis(1000));
        log.info("Count: {}", rs.count());

        for (ConsumerRecord<String, String> msg : rs) {
          log.info("Msg: {} - {} - {}", msg.key(), msg.value(), msg.offset());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (consumer != null) {
        consumer.close();
      }
    }
  }
}
