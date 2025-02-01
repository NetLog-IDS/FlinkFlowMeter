package id.ac.ui.cs.netlog.operators;

import org.apache.flink.api.common.functions.MapFunction;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.data.packets.Layers;
import id.ac.ui.cs.netlog.data.packets.Packet;
import id.ac.ui.cs.netlog.data.packets.TCP;
import id.ac.ui.cs.netlog.data.packets.UDP;
import id.ac.ui.cs.netlog.utils.PacketUtils;

public class ExtractPacketInfo implements MapFunction<Packet, PacketInfo> {
    @Override
    public PacketInfo map(Packet packet) {
        return this.getIpv4Info(packet);
    }

    private PacketInfo getIpv4Info(Packet packet){
		PacketInfo packetInfo = null;		
		try {
            Layers packetLayer = packet.getLayers();
			if (packetLayer.getNetwork() != null){
				packetInfo = new PacketInfo();
				packetInfo.setSrc(PacketUtils.ipToByteArray(packetLayer.getNetwork().getSrc()));
				packetInfo.setDst(PacketUtils.ipToByteArray(packetLayer.getNetwork().getDst()));
				packetInfo.setTimeStamp(packet.getTimestamp());

				if(packetLayer.getTransport() instanceof TCP){
                    TCP tcp = (TCP) packetLayer.getTransport();
					packetInfo.setTCPWindow(tcp.getWindow());
					packetInfo.setSrcPort(tcp.getSrcPort());
					packetInfo.setDstPort(tcp.getDstPort());
					packetInfo.setProtocol(6);
					packetInfo.setFlagFIN((tcp.getFlags() & 1) != 0);
                    packetInfo.setFlagSYN((tcp.getFlags() & 2) != 0);
                    packetInfo.setFlagRST((tcp.getFlags() & 4) != 0);
					packetInfo.setFlagPSH((tcp.getFlags() & 8) != 0);
                    packetInfo.setFlagACK((tcp.getFlags() & 16) != 0);
					packetInfo.setFlagURG((tcp.getFlags() & 32) != 0);
					packetInfo.setFlagECE((tcp.getFlags() & 64) != 0);
					packetInfo.setFlagCWR((tcp.getFlags() & 128) != 0);
					packetInfo.setPayloadBytes(tcp.getPayloadLength());
					packetInfo.setHeaderBytes(tcp.getHeaderLength());
				} else if(packetLayer.getTransport() instanceof UDP){
                    UDP udp = (UDP) packetLayer.getTransport();
					packetInfo.setSrcPort(udp.getSrcPort());
					packetInfo.setDstPort(udp.getDstPort());
                    packetInfo.setProtocol(17);
					packetInfo.setPayloadBytes(udp.getPayloadLength());
					packetInfo.setHeaderBytes(udp.getHeaderLength());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return packetInfo;
	}
}
