package id.ac.ui.cs.netlog.operators;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.Flow;
import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.data.cicflowmeter.ProtocolEnum;
import id.ac.ui.cs.netlog.data.cicflowmeter.TCPFlowState;
import id.ac.ui.cs.netlog.utils.TimeUtils;

public class FlowGenerator extends KeyedProcessFunction<String, PacketInfo, Flow> {
    //total 85 colums
	/*public static final String timeBasedHeader = "Flow ID, Source IP, Source Port, Destination IP, Destination Port, Protocol, "
			+ "Timestamp, Flow Duration, Total Fwd Packets, Total Backward Packets,"
			+ "Total Length of Fwd Packets, Total Length of Bwd Packets, "
			+ "Fwd Packet Length Max, Fwd Packet Length Min, Fwd Packet Length Mean, Fwd Packet Length Std,"
			+ "Bwd Packet Length Max, Bwd Packet Length Min, Bwd Packet Length Mean, Bwd Packet Length Std,"
			+ "Flow Bytes/s, Flow Packets/s, Flow IAT Mean, Flow IAT Std, Flow IAT Max, Flow IAT Min,"
			+ "Fwd IAT Total, Fwd IAT Mean, Fwd IAT Std, Fwd IAT Max, Fwd IAT Min,"
			+ "Bwd IAT Total, Bwd IAT Mean, Bwd IAT Std, Bwd IAT Max, Bwd IAT Min,"
			+ "Fwd PSH Flags, Bwd PSH Flags, Fwd URG Flags, Bwd URG Flags, Fwd Header Length, Bwd Header Length,"
			+ "Fwd Packets/s, Bwd Packets/s, Min Packet Length, Max Packet Length, Packet Length Mean, Packet Length Std, Packet Length Variance,"
			+ "FIN Flag Count, SYN Flag Count, RST Flag Count, PSH Flag Count, ACK Flag Count, URG Flag Count, "
			+ "CWR Flag Count, ECE Flag Count, Down/Up Ratio, Average Packet Size, Avg Fwd Segment Size, Avg Bwd Segment Size, Fwd Header Length,"
			+ "Fwd Avg Bytes/Bulk, Fwd Avg Packets/Bulk, Fwd Avg Bulk Rate, Bwd Avg Bytes/Bulk, Bwd Avg Packets/Bulk,"
			+ "Bwd Avg Bulk Rate,"
			+ "Subflow Fwd Packets, Subflow Fwd Bytes, Subflow Bwd Packets, Subflow Bwd Bytes,"
			+ "Init_Win_bytes_forward, Init_Win_bytes_backward, act_data_pkt_fwd, min_seg_size_forward,"
			+ "Active Mean, Active Std, Active Max, Active Min,"
			+ "Idle Mean, Idle Std, Idle Max, Idle Min, Label";*/

    private static final Long FLOW_TIMEOUT = 120000000L;
    private static final Long ACTIVITY_TIMEOUT = 5000000L;
	private static final List<ProtocolEnum> TCP_UDP_LIST_FILTER = Arrays.asList(ProtocolEnum.TCP, ProtocolEnum.UDP);

	private transient ValueState<Flow> flowState;    

	private boolean bidirectional;
	
	public FlowGenerator(boolean bidirectional) {
		this.bidirectional = bidirectional;
	}

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Flow> descriptor = new ValueStateDescriptor<>(
			"flowState",
			TypeInformation.of(new TypeHint<Flow>() {})
		);
		flowState = getRuntimeContext().getState(descriptor);
	}

	@Override
    public void processElement(PacketInfo packet,
            KeyedProcessFunction<String, PacketInfo, Flow>.Context ctx, Collector<Flow> out)
            throws Exception {
        if (packet == null) return;

		// System.out.println("=== PACKET START FOR " + packet.getFlowBidirectionalId() + " ===");

		Flow flow = flowState.value();
		Long currentInstanceTimestamp = TimeUtils.getCurrentTimeMicro();
		
    	if (flow != null) {
            Long currentTimestamp = packet.getTimeStamp();

			// Flow finished due flowtimeout:
            // 1.- we move the flow to finished flow list
            // 2.- we eliminate the flow from the current flow list
            // 3.- we create a new flow with the packet-in-process
    		if ((currentTimestamp - flow.getFlowStartTime()) > FLOW_TIMEOUT
					|| ((flow.getTcpFlowState() == TCPFlowState.READY_FOR_TERMINATION) && packet.isFlagSYN())) {

				// set cumulative flow time if TCP packet
                if (TCP_UDP_LIST_FILTER.contains(flow.getProtocol())) {
                    long currDuration = flow.getCumulativeConnectionDuration();
                    currDuration += flow.getFlowDuration();
                    flow.setCumulativeConnectionDuration(currDuration);
                }

				out.collect(flow);

				// Create a new UDP flow if activity time difference between the current UDP
                // packet, and the last
                // packet in the previous flow is greater than the flow activity timeout. This
                // is to soften the issue
                // with hard separation UDP flows that are likely part of the same "dialogue",
                // which can lead to single
                // packet flows with the hard flow time out cutoff. The concept of a "dialogue"
                // is not well-defined in
                // UDP, like TCP, so we assume that if the activity time difference between the
                // current packet and the
                // last packet in the previous flow is greater than the flow activity timeout,
                // then the current packet
                // is part of a new "dialogue".
                boolean createNewUdpFlow = (flow.getProtocol() == ProtocolEnum.UDP
                        && currentTimestamp - flow.getLastSeen() > FLOW_TIMEOUT);

                // If the original flow is set for termination, or the flow is not a tcp
                // connection, create a new flow,
                // and place it into the currentFlows list
                // Having a SYN packet and no ACK packet means it's the first packet in a new
                // flow
                if ((flow.getTcpFlowState() == TCPFlowState.READY_FOR_TERMINATION && packet.isFlagSYN()) // tcp flow is
                                                                                                          // ready for
                                                                                                          // termination
                        || createNewUdpFlow // udp packet is not part of current "dialogue"
                        || !TCP_UDP_LIST_FILTER.contains(packet.getProtocol()) // other protocols
                ) {
                    if (packet.isFlagSYN() && packet.isFlagACK()) {
                        // create new flow, switch direction - we assume the PCAP file had a mistake
                        // where SYN-ACK arrived before SYN packet
						flowState.update(new Flow(
							currentInstanceTimestamp,
							bidirectional,
							packet,
							packet.getDst(),
							packet.getSrc(),
							packet.getDstPort(),
							packet.getSrcPort(),
							ACTIVITY_TIMEOUT
						));
                    } else {
                        // Packet only has SYN, no ACK
						flowState.update(new Flow(
							currentInstanceTimestamp,
							bidirectional,
							packet,
							packet.getSrc(),
							packet.getDst(),
							packet.getSrcPort(),
							packet.getDstPort(),
							ACTIVITY_TIMEOUT
						));
                    }
                } else {
                    // Otherwise, the previous flow was likely terminated because of a timeout, and
                    // the new flow has to
                    // maintain the same source and destination information as the previous flow
                    // (since they're part of the
                    // same TCP connection or UDP "dialogue".
                    Flow newFlow = new Flow(
						currentInstanceTimestamp,
						bidirectional, packet,
						flow.getSrc(),
						flow.getDst(),
                        flow.getSrcPort(),
                        flow.getDstPort(),
						ACTIVITY_TIMEOUT,
						flow.getTcpPacketsSeen()
					);

                    long currDuration = flow.getCumulativeConnectionDuration();
                    // get the gap between the last flow and the start of this flow
                    currDuration += (currentTimestamp - flow.getLastSeen());
                    newFlow.setCumulativeConnectionDuration(currDuration);
					flowState.update(newFlow);
                }
				triggerTimer(packet, ctx);
    		} else if (packet.isFlagFIN()) {
                // Flow finished due FIN flag (tcp only):
                // 1.- we add the packet-in-process to the flow (it is the last packet)
                // 2.- we move the flow to finished flow list
                // 3.- we eliminate the flow from the current flow list

                flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
                flow.addPacket(packet);

                // First FIN packet
                if (flow.getTcpFlowState() == null) {
                    flow.setTcpFlowState(TCPFlowState.FIRST_FIN_FLAG_RECEIVED);
                } else if (flow.getTcpFlowState() == TCPFlowState.FIRST_FIN_FLAG_RECEIVED) {
                    // Second FIN packet
                    if (flow.getFwdFINFlags() > 0 && flow.getBwdFINFlags() > 0) {
                        flow.setTcpFlowState(TCPFlowState.SECOND_FIN_FLAG_RECEIVED);
                    }
                }

				flowState.update(flow);         
    		} else if (packet.isFlagRST()) {
				flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
                flow.addPacket(packet);
                flow.setTcpFlowState(TCPFlowState.READY_FOR_TERMINATION);
				// TODO: make it submit without waiting for next packets -> problem: between READY FOR TERMINATION and next SYN, there may be packets
				// If you want to drop it (e.g. drop until SYN/Timeout), you need to regenerate the dataset too
				// TODO: Oh! what if we just make the timer trigger time as min(flow timeout, current time - packet time)
                flowState.update(flow);
			} else if (packet.isFlagACK()) {
				flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
                flow.addPacket(packet);

                // Final ack packet for TCP flow termination
                if (flow.getTcpFlowState() == TCPFlowState.SECOND_FIN_FLAG_RECEIVED) {
                    flow.setTcpFlowState(TCPFlowState.READY_FOR_TERMINATION); // TODO: see above
                }
                flowState.update(flow);
			} else if (flow.getProtocol() == ProtocolEnum.ICMP) {
				// create a new flow if the icmp code and types are different
				if (flow.getIcmpCode() != packet.getIcmpCode() && flow.getIcmpType() != packet.getIcmpType()) {
					// finish existing flow
					out.collect(flow);

					// create new flow
					flowState.update(new Flow(
						currentInstanceTimestamp,
						bidirectional,
						packet,
						packet.getSrc(),
						packet.getDst(),
						packet.getSrcPort(),
						packet.getDstPort(),
						ACTIVITY_TIMEOUT
					));
					triggerTimer(packet, ctx);
				} else {
					// normal behavior
					flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
					flow.addPacket(packet);
					flowState.update(flow);
				}
			} else { // default
    			flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
                flow.addPacket(packet);
                flowState.update(flow);
    		}
    	} else {
			if (packet.isFlagSYN() && packet.isFlagACK()) {
				// Backward
				// TODO: make sure kalo dst src kebalik bakal dianggap backward
				flowState.update(new Flow(
					currentInstanceTimestamp,
					bidirectional,
					packet,
					packet.getDst(),
					packet.getSrc(),
					packet.getDstPort(),
					packet.getSrcPort(),
					ACTIVITY_TIMEOUT
				));
            } else {
				// Forward
				// TODO: make sure kalo dst src sesuai bakal dianggap forward
				flowState.update(new Flow(
					currentInstanceTimestamp,	
					bidirectional,
					packet,
					ACTIVITY_TIMEOUT
				));
            }
			triggerTimer(packet, ctx);
    	}

		// System.out.println("=== PACKET ENDED FOR " + packet.getFlowBidirectionalId() + " ===");
    }

	// TODO: maybe we should use system time instead for epoch
	private void triggerTimer(PacketInfo packet, KeyedProcessFunction<String, PacketInfo, Flow>.Context ctx) {
		// System.out.println(packet.getFlowBidirectionalId() + " TRIGGERING TIMER SERVICE");
		long diffMilli = FLOW_TIMEOUT / 1000L;
		long triggerTime = ctx.timerService().currentProcessingTime() + diffMilli;
		// System.out.println("CURRENT FLINK TIME:");
		// System.out.println(ctx.timerService().currentProcessingTime());
		// System.out.println("CURRENT PACKET TIME:");
		// System.out.println(packet.getTimeStamp());
		ctx.timerService().registerProcessingTimeTimer(triggerTime);
	}

	// TODO: there's concern about unsynchronized time which makes timestamp - flow.getFlowStartTime() < 0
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Flow> out) throws Exception {
		// System.out.println("==TIME TRIGGER START");
		Flow flow = flowState.value();
		if (flow == null) return;

		Long startTimeMilli = flow.getFlowStartTime() / 1000L;

		// System.out.println("FLINK TIME: " + timestamp);
		// System.out.println("PACKET TIME: " + startTimeMilli);

		// Flow finished due flowtimeout: 
		// 1.- we move the flow to finished flow list
		// 2.- we eliminate the flow from the current flow list
		// 3.- we create a new flow with the packet-in-process
		// TODO: if startTimeMilli = flow start time and not flink start time --> will always be true --> need to cancel last timer
		if ((timestamp - startTimeMilli) >= (FLOW_TIMEOUT / 1000L)) { // TODO: sesuain whole block ini (isinya jg) sama yg fixed CIC
			// set cumulative flow time if TCP packet
			if (TCP_UDP_LIST_FILTER.contains(flow.getProtocol())) {
				long currDuration = flow.getCumulativeConnectionDuration();
				currDuration += flow.getFlowDuration();
				flow.setCumulativeConnectionDuration(currDuration);
			}
			
			// System.out.println(flow.packetCount());
			// if (flow.packetCount() > 1) {
			// System.out.println("COLLECTED");
			out.collect(flow);
			// }
			flowState.update(null);
		}

		// System.out.println("==TIME TRIGGER END");
    }
}
