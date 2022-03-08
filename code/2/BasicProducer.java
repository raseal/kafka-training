package rrsesino.kafka.productor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.Future;

public class BasicProducer
{
  private static final Logger log = LoggerFactory.getLogger(BasicProducer.class);

  public static void main(String[] args)
  {
    Properties prop = new Properties();
    String topicName = "basic-producer";

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

    // Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

    // Message
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(
      topicName,
      "key-1",
      "My second message"
    );

    // Send
    Future<RecordMetadata> rs = producer.send(record);

    try {
      RecordMetadata metadata = rs.get();
      log.info(
        "Msg topic:{} -- partition:{} -- Offset:{} -- Timestamp:{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset(),
        metadata.timestamp()
      );

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
