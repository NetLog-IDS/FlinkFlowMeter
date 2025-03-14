package id.ac.ui.cs.netlog.sinks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;

public interface StreamSink {
    DataStreamSink<FlowStats> applySink(DataStream<FlowStats> stream);
}
