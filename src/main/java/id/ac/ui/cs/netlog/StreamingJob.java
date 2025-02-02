package id.ac.ui.cs.netlog;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;
import id.ac.ui.cs.netlog.data.packets.Packet;
import id.ac.ui.cs.netlog.operators.ExtractFlowStats;
import id.ac.ui.cs.netlog.operators.ExtractPacketInfo;
import id.ac.ui.cs.netlog.operators.FlowGenerator;
import id.ac.ui.cs.netlog.operators.PacketParser;
import id.ac.ui.cs.netlog.serialization.deserializers.PacketDeserializationSchema;

public class StreamingJob {
	private static final String SOURCE_TYPE = "source";
	private static final String SOURCE_KAFKA_SERVERS = "servers";
	private static final String SOURCE_KAFKA_TOPIC = "topic";
	private static final String SOURCE_GROUP_ID = "group";
	private static final String SOURCE_SOCKET_HOST = "host";
	private static final String SOURCE_SOCKET_PORT = "port";
	

    public static void main(String[] args) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Packet> source;
        if (parameters.get(SOURCE_TYPE, "socket").equals("kafka")) {
			source = getKafkaSource(env, parameters, objectMapper);
		} else {
			source = getSocketSource(env, parameters, objectMapper);
		}

        DataStream<FlowStats> stream = source
				.map(new ExtractPacketInfo())
				.keyBy(packetInfo -> packetInfo.getFlowBidirectionalId())
				.process(new FlowGenerator(true))
				.map(new ExtractFlowStats());
				stream.print();

		env.execute("FlinkFlowMeter");
    }

	private static DataStream<Packet> getKafkaSource(StreamExecutionEnvironment env, ParameterTool parameters, ObjectMapper objectMapper) {
		String servers = parameters.get(SOURCE_KAFKA_SERVERS, "localhost:9200");
		String topic = parameters.get(SOURCE_KAFKA_TOPIC, "network-flows");
		String groupId = parameters.get(SOURCE_GROUP_ID, "flink-1");

		KafkaSource<Packet> source = KafkaSource.<Packet>builder()
			.setBootstrapServers(servers)
			.setTopics(topic)
			.setGroupId(groupId)
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new PacketDeserializationSchema(objectMapper))
			.build();

		return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
	}

	private static DataStream<Packet> getSocketSource(StreamExecutionEnvironment env, ParameterTool parameters, ObjectMapper objectMapper) {
		String host = parameters.get(SOURCE_SOCKET_HOST, "host.docker.internal");
		Integer port = Integer.parseInt(parameters.get(SOURCE_SOCKET_PORT, "9999"));

		return env.socketTextStream(host, port).map(new PacketParser(objectMapper));
	}
}
