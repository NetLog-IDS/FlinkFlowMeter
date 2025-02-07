package id.ac.ui.cs.netlog.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.packets.Packet;
import id.ac.ui.cs.netlog.serialization.deserializers.PacketDeserializationSchema;

public class KafkaStreamSource implements StreamSource {
    private static final String KAFKA_SERVERS = "source-servers";
	private static final String KAFKA_TOPIC = "source-topic";
	private static final String GROUP_ID = "source-group";

    private final String servers;
    private final String topic;
    private final String groupId;
	private final StreamExecutionEnvironment env;
	private final ObjectMapper objectMapper;

    public KafkaStreamSource(StreamExecutionEnvironment env, ParameterTool parameters, ObjectMapper objectMapper) {
        this.servers = parameters.get(KAFKA_SERVERS, "localhost:9200");
        this.topic = parameters.get(KAFKA_TOPIC, "network-traffic");
		this.groupId = parameters.get(GROUP_ID, "flink-1");
		System.out.println(this.servers);
		System.out.println(this.topic);
		System.out.println(this.groupId);
		this.env = env;
		this.objectMapper = objectMapper;
    }

	@Override
    public DataStream<Packet> getSourceStream() {
		KafkaSource<Packet> source = KafkaSource.<Packet>builder()
			.setBootstrapServers(this.servers)
			.setTopics(this.topic)
			.setGroupId(this.groupId)
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new PacketDeserializationSchema(this.objectMapper))
			.build();
		return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
	}
}
