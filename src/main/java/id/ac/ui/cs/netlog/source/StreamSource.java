package id.ac.ui.cs.netlog.source;

import org.apache.flink.streaming.api.datastream.DataStream;

import id.ac.ui.cs.netlog.data.packets.Packet;

public interface StreamSource {
    DataStream<Packet> getSourceStream();
}
