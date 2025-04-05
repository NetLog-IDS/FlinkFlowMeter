package id.ac.ui.cs.netlog.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.data.cicflowmeter.ProtocolEnum;
import id.ac.ui.cs.netlog.data.packets.Layers;
import id.ac.ui.cs.netlog.data.packets.Packet;
import id.ac.ui.cs.netlog.data.packets.TCP;
import id.ac.ui.cs.netlog.data.packets.UDP;
import id.ac.ui.cs.netlog.utils.PacketUtils;

public class ExtractPacketInfoFromList implements MapFunction<List<Packet>, List<PacketInfo>> {
    @Override
    public List<PacketInfo> map(List<Packet> packets) {
        List<PacketInfo> infos = new ArrayList<>();
        for (Packet packet: packets) {
            infos.add(this.getIpv4Info(packet));
        }
        return infos;
    }

    private PacketInfo getIpv4Info(Packet packet){
		PacketInfo packetInfo = null;		
		try {
            Layers packetLayer = packet.getLayers();
			if (packetLayer.getNetwork() != null){
				packetInfo = new PacketInfo();
				packetInfo.setSrc(PacketUtils.ipToByteArray(packetLayer.getNetwork().getSrc()));
				packetInfo.setDst(PacketUtils.ipToByteArray(packetLayer.getNetwork().getDst()));
				packetInfo.setOrder(packet.getOrder());
				packetInfo.setPublisherId(packet.getPublisherId());
				packetInfo.setTimeStamp(packet.getTimestamp());

				if (packetLayer.getTransport() instanceof TCP) {
                    TCP tcp = (TCP) packetLayer.getTransport();
					packetInfo.setTCPWindow(tcp.getWindow());
					packetInfo.setSrcPort(tcp.getSrcPort());
					packetInfo.setDstPort(tcp.getDstPort());
					packetInfo.setProtocol(ProtocolEnum.TCP);
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
				} else if (packetLayer.getTransport() instanceof UDP) {
                    UDP udp = (UDP) packetLayer.getTransport();
					packetInfo.setSrcPort(udp.getSrcPort());
					packetInfo.setDstPort(udp.getDstPort());
					packetInfo.setPayloadBytes(udp.getPayloadLength());
					packetInfo.setHeaderBytes(udp.getHeaderLength());
					packetInfo.setProtocol(ProtocolEnum.UDP);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return packetInfo;
	}
}
