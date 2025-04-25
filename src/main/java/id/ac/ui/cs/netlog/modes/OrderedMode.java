package id.ac.ui.cs.netlog.modes;

import org.apache.flink.streaming.api.datastream.DataStream;

import id.ac.ui.cs.netlog.data.cicflowmeter.optimized.FlowStats;
import id.ac.ui.cs.netlog.operators.ExtractFlowStats;
import id.ac.ui.cs.netlog.operators.ExtractPacketInfo;
import id.ac.ui.cs.netlog.operators.FlowGenerator;
import id.ac.ui.cs.netlog.operators.OptimizedFlowGenerator;
import id.ac.ui.cs.netlog.source.StreamSource;

public class OrderedMode implements StreamMode {
    @Override
    public DataStream<FlowStats> createPipeline(StreamSource source) {
        return source.getSourceStream()
				.flatMap(new ExtractPacketInfo())
				.keyBy(packetInfo -> {
					if (packetInfo == null) {
						System.out.println("[UNEXPECTED] Packet Info is null");
					}
					String id = packetInfo.getPublisherId() + "-" + packetInfo.getFlowBidirectionalId();
					return id;
				})
				// .process(new FlowGenerator(true))
				.process(new OptimizedFlowGenerator(true))
				.map(new ExtractFlowStats());
    }
}
