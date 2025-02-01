package id.ac.ui.cs.netlog;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;
import id.ac.ui.cs.netlog.operators.ExtractFlowStats;
import id.ac.ui.cs.netlog.operators.ExtractPacketInfo;
import id.ac.ui.cs.netlog.operators.FlowGenerator;
import id.ac.ui.cs.netlog.operators.PacketParser;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ObjectMapper objectMapper = new ObjectMapper();

        DataStream<FlowStats> stream = env.socketTextStream("host.docker.internal", 9999)
				.map(new PacketParser(objectMapper))
				.map(new ExtractPacketInfo())
				.keyBy(packetInfo -> packetInfo.getFlowBidirectionalId())
				.process(new FlowGenerator(true))
				.map(new ExtractFlowStats());
				stream.print();

		env.execute("FlinkFlowMeter");
    }
}
