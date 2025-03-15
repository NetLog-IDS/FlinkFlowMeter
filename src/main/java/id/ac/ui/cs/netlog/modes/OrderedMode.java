package id.ac.ui.cs.netlog.modes;

import org.apache.flink.streaming.api.datastream.DataStream;

import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;
import id.ac.ui.cs.netlog.operators.ExtractFlowStats;
import id.ac.ui.cs.netlog.operators.ExtractPacketInfo;
import id.ac.ui.cs.netlog.operators.FlowGenerator;
import id.ac.ui.cs.netlog.source.StreamSource;

public class OrderedMode implements StreamMode {
    @Override
    public DataStream<FlowStats> createPipeline(StreamSource source) {
        return source.getSourceStream()
				.map(new ExtractPacketInfo())
				.keyBy(packetInfo -> {
					String id = packetInfo.getPublisherId() + "-" + packetInfo.getFlowBidirectionalId();
					System.out.println(id);
					return id;
				})
				.process(new FlowGenerator(true))
				.map(new ExtractFlowStats());
    }
}
