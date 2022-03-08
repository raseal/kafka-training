package rrsesino.kafka.productor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class InterceptorProducer
{
  private static final Logger log = LoggerFactory.getLogger(InterceptorProducer.class);

  public static void main(String[] args)
  {
    Properties prop = new Properties();
    String topicName = "basic-producer";

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomInterceptor.class.getCanonicalName());

    // Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

    // Headers
    List<Header> headers = new ArrayList<>();
    headers.add(new RecordHeader("mykey", "value".getBytes()));

    // Message
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(
      topicName,
      0,
      "key-1",
      "My intercepted second message",
      headers
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
