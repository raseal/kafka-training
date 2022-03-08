package rrsesino.kafka.productor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class AsyncProducer {
  private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

  public static void main(String[] args) {
    try {
      Properties prop = new Properties();
      String topicName = "basic-producer";

      prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      prop.put(ProducerConfig.CLIENT_ID_CONFIG, "ServiceName-Hostname");

      // Producer
      KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

      // Message
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(
        topicName,
        "key-1",
        "My async message"
      );

      // Send
      // For async we must define a callback
      Callback callback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
          if (e != null) {
            e.printStackTrace();
          } else {
            log.info(
              "Msg topic callback:{} -- partition:{} -- Offset:{} -- Timestamp:{}",
              metadata.topic(),
              metadata.partition(),
              metadata.offset(),
              metadata.timestamp()
            );
          }
        }
      };

      // We no longer need to wait for response
      producer.send(record, callback);
      log.info("Async message sent!");
      Thread.sleep(2000L);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
