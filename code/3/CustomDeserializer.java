package rrsesino.kafka.productor.consumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Locale;
import java.util.Map;

public class CustomDeserializer implements Deserializer<String> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public String deserialize(String topic, byte[] bytes) {
    return new String(bytes).toUpperCase(Locale.ROOT);
  }

  @Override
  public String deserialize(String topic, Headers headers, byte[] data) {
    return deserialize(topic, data);
  }

  @Override
  public void close() {
  }
}
