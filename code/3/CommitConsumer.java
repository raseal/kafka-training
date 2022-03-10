package rrsesino.kafka.productor.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rrsesino.kafka.productor.AsyncProducer;

import java.time.Duration;
import java.util.*;

public class CommitConsumer {
  private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "CommitConsumer");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

    // option 1
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // required for option 1
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100"); // we can force a commit when this amount of time is elapsed

    // required for option 2,3,4,5,6
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> consumer = null;

    List<String> topics = new ArrayList<>();
    topics.add("basic-producer");

    try {
      consumer = new KafkaConsumer<String, String>(config);
      consumer.subscribe(topics);

      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();

      while(true) {
        log.info("Log before poll");
        ConsumerRecords<String, String> rs = consumer.poll(Duration.ofMillis(1000));
        log.info("Count: {}", rs.count());

        for (ConsumerRecord<String, String> msg : rs) {
          log.info("Msg: {} - {} - {}", msg.key(), msg.value(), msg.offset());
          //option3: partial commits: it has a very high impact on performance :(
          offsets.put(new TopicPartition(msg.topic(), msg.partition()), new OffsetAndMetadata(msg.offset()));
          consumer.commitSync(offsets);
          // option 4: use commitAsync() instead of the sync: it is fast BUT we can not assure the commit is properly managed by the server :(

          // option 5: use commitSync() every X messages. It is still SLOW and there are corner-cases (what if you have a prime number of messages?)

          // option 6: the recommended one: we perform a commitAsync after each message and in the end of the FOR we execute a commitSync
        }
        consumer.commitSync(); // option 2: manual commit :(
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
