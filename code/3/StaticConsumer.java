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

public class StaticConsumer {
  private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "StaticConsumer");
    config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "ID-1"); // We have a limited amount of consumers: we can NOT create two consumers with the same ID
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
