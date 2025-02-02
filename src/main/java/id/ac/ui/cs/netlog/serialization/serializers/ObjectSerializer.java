package id.ac.ui.cs.netlog.serialization.serializers;

import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ObjectSerializer<T> implements KafkaRecordSerializationSchema<T> {
    private final String topic;
    private final ObjectMapper mapper;

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(T element, KafkaSinkContext context, Long timestamp) {
        try {
            return new ProducerRecord<>(
                topic,
                UUID.randomUUID().toString().getBytes(),
                mapper.writeValueAsBytes(element)
            );
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
