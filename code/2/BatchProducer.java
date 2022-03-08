package rrsesino.kafka.productor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class BatchProducer
{
  private static final Logger log = LoggerFactory.getLogger(BatchProducer.class);

  public static void main(String[] args)
  {
    Properties prop = new Properties();
    String topicName = "basic-producer-2";

    try {
      prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      prop.put(ProducerConfig.LINGER_MS_CONFIG, "20000");

      // Producer
      KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

      for (int i = 0; i < 10; i++) {
        // Message
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
          topicName,
          "key-1",
          "My batched message"
        );

        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            log.info("Msg sent");
          }
        });
      }

      Thread.sleep(60000L);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
