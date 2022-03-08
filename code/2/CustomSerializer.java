package rrsesino.kafka.productor;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

public class CustomSerializer implements Serializer<String> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, String data) {
    String content = topic + "|" + data;

    return content.toUpperCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, String data) {
    return serialize(topic, data);
  }

  @Override
  public void close() {}
}
