package id.ac.ui.cs.netlog.modes;

import org.apache.flink.streaming.api.datastream.DataStream;

import id.ac.ui.cs.netlog.data.cicflowmeter.optimized.FlowStats;
import id.ac.ui.cs.netlog.source.StreamSource;

public interface StreamMode {
    DataStream<FlowStats> createPipeline(StreamSource source);
}
