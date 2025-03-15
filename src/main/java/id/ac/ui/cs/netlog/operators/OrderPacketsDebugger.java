package id.ac.ui.cs.netlog.operators;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;

public class OrderPacketsDebugger implements FlatMapFunction<List<PacketInfo>, List<PacketInfo>> {
    @Override
    public void flatMap(List<PacketInfo> packets, Collector<List<PacketInfo>> out) throws Exception {
        System.out.print("[PACKET_OBTAINED] ");
        for (PacketInfo packet : packets) {
            System.out.print(packet.getOrder());
            System.out.print(", ");
        }
        System.out.println();
        out.collect(packets);
    }
}
