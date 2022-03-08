package rrsesino.kafka.productor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomInterceptor implements ProducerInterceptor<String, String> {

  private static final Logger log = LoggerFactory.getLogger(BasicHeaderProducer.class);

  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
    log.info("onSend:{}", producerRecord);

    return producerRecord;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    log.info("ACK: {}", recordMetadata);
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> map) {}
}
