package id.ac.ui.cs.netlog.modes;

import org.apache.flink.streaming.api.datastream.DataStream;

import id.ac.ui.cs.netlog.data.cicflowmeter.optimized.FlowStats;
import id.ac.ui.cs.netlog.operators.ExtractFlowStats;
import id.ac.ui.cs.netlog.operators.ExtractPacketInfo;
import id.ac.ui.cs.netlog.operators.FlowGeneratorFromList;
import id.ac.ui.cs.netlog.operators.OrderPackets;
import id.ac.ui.cs.netlog.operators.OrderPacketsDebugger;
import id.ac.ui.cs.netlog.source.StreamSource;

public class UnorderedMode implements StreamMode {
    @Override
    public DataStream<FlowStats> createPipeline(StreamSource source) {
		return null;
        // return source.getSourceStream()
		// 		.flatMap(new ExtractPacketInfo())
		// 		.keyBy(packetInfo -> {
		// 			String id = packetInfo.getPublisherId() + "-" + packetInfo.getFlowBidirectionalId();
		// 			// System.out.println(id);
		// 			return id;
		// 		})
		// 		.process(new OrderPackets())
		// 		.flatMap(new OrderPacketsDebugger())
        //         .flatMap(new FlowGeneratorFromList(true))
        //         .map(new ExtractFlowStats());
    }
}
