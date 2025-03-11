package id.ac.ui.cs.netlog;

import java.util.List;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;
import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.operators.ExtractFlowStats;
import id.ac.ui.cs.netlog.operators.ExtractPacketInfo;
import id.ac.ui.cs.netlog.operators.FlowGenerator;
import id.ac.ui.cs.netlog.operators.OrderPackets;
import id.ac.ui.cs.netlog.operators.OrderPacketsDebugger;
import id.ac.ui.cs.netlog.serialization.serializers.ObjectSerializer;
import id.ac.ui.cs.netlog.source.StreamSource;
import id.ac.ui.cs.netlog.source.enums.StreamSourceEnum;
import id.ac.ui.cs.netlog.source.factory.StreamSourceFactory;

public class StreamingJob {
	private static final String SOURCE_TYPE = "source";

	private static final String SINK_TYPE = "sink";
	private static final String SINK_KAFKA_SERVERS = "sink-servers";
	private static final String SINK_KAFKA_TOPIC = "sink-topic";

    public static void main(String[] args) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String sourceString = parameters.get(SOURCE_TYPE, StreamSourceEnum.SOCKET.toString());
		System.out.println(sourceString);

		StreamSourceFactory factory = new StreamSourceFactory(objectMapper);
		StreamSource source = factory.getStreamSource(StreamSourceEnum.fromString(sourceString), env, parameters);

        // DataStream<FlowStats> stream = source.getSourceStream()
		// 		.map(new ExtractPacketInfo())
		// 		.keyBy(packetInfo -> {
		// 			String id = packetInfo.getFlowBidirectionalId();
		// 			System.out.println(id);
		// 			return id;
		// 		})
		// 		.process(new FlowGenerator(true))
		// 		.map(new ExtractFlowStats());

		DataStream<List<PacketInfo>> stream = source.getSourceStream()
				.map(new ExtractPacketInfo())
				.keyBy(packetInfo -> {
					String id = packetInfo.getPublisherId() + "-" + packetInfo.getFlowBidirectionalId();
					System.out.println(id);
					return id;
				})
				.process(new OrderPackets())
				.flatMap(new OrderPacketsDebugger());
		
		stream.print();

		// if (parameters.get(SINK_TYPE, "print").equals("kafka")) {
		// 	kafkaSink(parameters, stream, objectMapper);
		// } else {
		// 	printSink(stream);
		// }

		env.execute("FlinkFlowMeter");
    }

	private static DataStreamSink<FlowStats> printSink(DataStream<FlowStats> stream) {
		return stream.print();
	}

	private static DataStreamSink<FlowStats> kafkaSink(ParameterTool parameters, DataStream<FlowStats> stream, ObjectMapper objectMapper) {
		String servers = parameters.get(SINK_KAFKA_SERVERS, "localhost:9200");
		String topic = parameters.get(SINK_KAFKA_TOPIC, "network-flows");

		KafkaSink<FlowStats> sink = KafkaSink.<FlowStats>builder()
			.setBootstrapServers(servers)
			.setRecordSerializer(new ObjectSerializer<FlowStats>(topic, objectMapper))
			.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
			.build();
		return stream.sinkTo(sink);
	}
}
