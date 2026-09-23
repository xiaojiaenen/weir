package io.weir.writer.kafka;

import io.weir.config.JobConfig;
import io.weir.model.DataRow;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka JSON sink. Key = primary key (for partition affinity), value = row JSON. Optional soft
 * delete uses field {@code _op=d}.
 */
public final class KafkaJsonWriter implements RowWriter {
  private final JobConfig config;
  private Producer<String, String> producer;
  private List<String> pk = List.of();

  public KafkaJsonWriter(JobConfig config) {
    this.config = config;
  }

  @Override
  public void open() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.target.bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, config.target.acks == null ? "all" : config.target.acks);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    if (config.target.options != null) {
      config.target.options.forEach(props::put);
    }
    pk =
        config.target.primaryKey != null && !config.target.primaryKey.isEmpty()
            ? config.target.primaryKey
            : (config.source.read.primaryKey == null ? List.of() : config.source.read.primaryKey);
    producer = new KafkaProducer<>(props);
  }

  @Override
  public void writeBatch(List<DataRow> batch) throws SQLException {
    if (batch.isEmpty()) {
      return;
    }
    try {
      for (DataRow row : batch) {
        String key = pk.isEmpty() ? String.valueOf(row.hashCode()) : keyOf(row);
        producer.send(
            new ProducerRecord<>(config.target.topic, key, toJson(row)),
            (meta, e) -> {
              if (e != null) {
                // async — surface via producer metrics / logs
                System.err.println("[weir-kafka] send failed: " + e.getMessage());
              }
            });
      }
      producer.flush();
    } catch (Exception e) {
      throw new SQLException("kafka write failed", e);
    }
  }

  private String keyOf(DataRow row) {
    if (pk.size() == 1) {
      return String.valueOf(row.get(pk.get(0)));
    }
    StringBuilder sb = new StringBuilder();
    for (String c : pk) {
      sb.append(row.get(c)).append('|');
    }
    return sb.toString();
  }

  private static String toJson(DataRow row) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, Object> e : row.values().entrySet()) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append('"').append(esc(e.getKey())).append("\":").append(val(e.getValue()));
    }
    return sb.append('}').toString();
  }

  private static String val(Object v) {
    if (v == null) {
      return "null";
    }
    if (v instanceof Number || v instanceof Boolean) {
      return String.valueOf(v);
    }
    if (v instanceof Instant i) {
      return '"' + i.toString() + '"';
    }
    return '"' + esc(String.valueOf(v)) + '"';
  }

  private static String esc(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }

  public static final class Factory implements RowWriterFactory {
    @Override
    public String type() {
      return "kafka";
    }

    @Override
    public RowWriter create(JobConfig config) {
      return new KafkaJsonWriter(config);
    }
  }
}
