package id.ac.ui.cs.netlog.sinks;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.cicflowmeter.optimized.FlowStats;
import id.ac.ui.cs.netlog.serialization.serializers.ObjectSerializer;

public class KafkaStreamSink implements StreamSink {
    private static final String SINK_KAFKA_SERVERS = "sink-servers";
	private static final String SINK_KAFKA_TOPIC = "sink-topic";

    private final String servers;
    private final String topic;
    private final ObjectMapper objectMapper;

    public KafkaStreamSink(ParameterTool parameters, ObjectMapper objectMapper) {
        this.servers = parameters.get(SINK_KAFKA_SERVERS, "localhost:9200");
		this.topic = parameters.get(SINK_KAFKA_TOPIC, "network-flows");
        this.objectMapper = objectMapper;
    }

    @Override
    public DataStreamSink<FlowStats> applySink(DataStream<FlowStats> stream) {
		KafkaSink<FlowStats> sink = KafkaSink.<FlowStats>builder()
			.setBootstrapServers(servers)
			.setRecordSerializer(new ObjectSerializer<FlowStats>(topic, objectMapper))
			.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
			.build();
		return stream.sinkTo(sink);
    }
}
