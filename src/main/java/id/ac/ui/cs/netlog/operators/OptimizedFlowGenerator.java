package id.ac.ui.cs.netlog.operators;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.optimized.Flow;
import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.data.cicflowmeter.ProtocolEnum;
import id.ac.ui.cs.netlog.data.cicflowmeter.TCPFlowState;
import id.ac.ui.cs.netlog.utils.TimeUtils;

public class OptimizedFlowGenerator extends KeyedProcessFunction<String, PacketInfo, Flow> {
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
	private transient MapState<Integer, Boolean> tcpSeenState;

	private boolean bidirectional;
	
	public OptimizedFlowGenerator(boolean bidirectional) {
		this.bidirectional = bidirectional;
	}

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Flow> flowDescriptor = new ValueStateDescriptor<>(
			"flowState",
			TypeInformation.of(new TypeHint<Flow>() {})
		);
		MapStateDescriptor<Integer, Boolean> tcpSeenDescriptor = new MapStateDescriptor<>(
			"tcpSeenState",
			TypeInformation.of(new TypeHint<Integer>() {}),
			TypeInformation.of(new TypeHint<Boolean>() {})
		);
		flowState = getRuntimeContext().getState(flowDescriptor);
		tcpSeenState = getRuntimeContext().getMapState(tcpSeenDescriptor);
	}

	@Override
    public void processElement(PacketInfo packet,
            KeyedProcessFunction<String, PacketInfo, Flow>.Context ctx, Collector<Flow> out)
            throws Exception {
        if (packet == null) return;
		if (!TCP_UDP_LIST_FILTER.contains(packet.getProtocol())) return;

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

				if (!flow.getSubmitted()) {
					updateCummulativeDuration(flow);
					out.collect(flow);
					flow.setSubmitted(true);
					untriggerSubmitTimer(flow, ctx);
				}
				untriggerClearanceTimer(flow, ctx); // new flow will be created, so it needs to be cleared

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
                        && currentTimestamp - flow.getFlowLastSeen() > FLOW_TIMEOUT);

                // If the original flow is set for termination, or the flow is not a tcp
                // connection, create a new flow,
                // and place it into the currentFlows list
                // Having a SYN packet and no ACK packet means it's the first packet in a new
                // flow
				Flow newFlow = null;
                if ((flow.getTcpFlowState() == TCPFlowState.READY_FOR_TERMINATION && packet.isFlagSYN()) // tcp flow is
                                                                                                          // ready for
                                                                                                          // termination
                        || createNewUdpFlow // udp packet is not part of current "dialogue"
                ) {
					tcpSeenState.clear();

                    if (packet.isFlagSYN() && packet.isFlagACK()) {
                        // create new flow, switch direction - we assume the PCAP file had a mistake
                        // where SYN-ACK arrived before SYN packet
						newFlow = new Flow(
							currentInstanceTimestamp,
							bidirectional,
							packet,
							packet.getDst(),
							packet.getSrc(),
							packet.getDstPort(),
							packet.getSrcPort(),
							ACTIVITY_TIMEOUT
						);
						updateRetransmission(newFlow, packet);
                    } else {
                        // Packet only has SYN, no ACK
						newFlow = new Flow(
							currentInstanceTimestamp,
							bidirectional,
							packet,
							packet.getSrc(),
							packet.getDst(),
							packet.getSrcPort(),
							packet.getDstPort(),
							ACTIVITY_TIMEOUT
						);
						updateRetransmission(newFlow, packet);
                    }
                } else {
                    // Otherwise, the previous flow was likely terminated because of a timeout, and
                    // the new flow has to
                    // maintain the same source and destination information as the previous flow
                    // (since they're part of the
                    // same TCP connection or UDP "dialogue".

					// tcpSeenState aren't cleared here.

                    newFlow = new Flow(
						currentInstanceTimestamp,
						bidirectional,
						packet,
						flow.getSrc(),
						flow.getDst(),
                        flow.getSrcPort(),
                        flow.getDstPort(),
						ACTIVITY_TIMEOUT
					);
					updateRetransmission(newFlow, packet);

                    long curDuration = flow.getCummulativeConnectionDuration();
                    curDuration += (currentTimestamp - flow.getFlowLastSeen()); // get the gap between the last flow and the start of this flow
                    newFlow.setCummulativeConnectionDuration(curDuration);
                }
				triggerTimer(newFlow, ctx);
				flowState.update(newFlow);
			} else if (flow.getTcpFlowState() == TCPFlowState.READY_FOR_TERMINATION) {
				// Ignore packets after termination and before SYN
				return;
    		} else if (packet.isFlagFIN()) {
                // Flow finished due FIN flag (tcp only):
                // 1.- we add the packet-in-process to the flow (it is the last packet)
                // 2.- we move the flow to finished flow list
                // 3.- we eliminate the flow from the current flow list

                flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
				updateRetransmission(flow, packet);
                flow.addPacket(packet);

                // First FIN packet
                if (flow.getTcpFlowState() == null) {
                    flow.setTcpFlowState(TCPFlowState.FIRST_FIN_FLAG_RECEIVED);
                } else if (flow.getTcpFlowState() == TCPFlowState.FIRST_FIN_FLAG_RECEIVED) {
                    // Second FIN packet
                    if (flow.getFwdFINCount() > 0 && flow.getBwdFINCount() > 0) {
                        flow.setTcpFlowState(TCPFlowState.SECOND_FIN_FLAG_RECEIVED);
                    }
                }

				flowState.update(flow);         
    		} else if (packet.isFlagRST()) {
				flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
				updateRetransmission(flow, packet);
                flow.addPacket(packet);
				flow.setTcpFlowState(TCPFlowState.READY_FOR_TERMINATION);
				updateCummulativeDuration(flow);
				out.collect(flow);
				flow.setSubmitted(true);
				untriggerSubmitTimer(flow, ctx);
                flowState.update(flow);
			} else if (packet.isFlagACK()) {
				flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
				updateRetransmission(flow, packet);
                flow.addPacket(packet);

                // Final ack packet for TCP flow termination
                if (flow.getTcpFlowState() == TCPFlowState.SECOND_FIN_FLAG_RECEIVED) {
                    flow.setTcpFlowState(TCPFlowState.READY_FOR_TERMINATION);
					updateCummulativeDuration(flow);
					out.collect(flow);
					flow.setSubmitted(true);
					untriggerSubmitTimer(flow, ctx);
                }
                flowState.update(flow);
			} else { // default
    			flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
				updateRetransmission(flow, packet);
                flow.addPacket(packet);
                flowState.update(flow);
    		}
    	} else {
			tcpSeenState.clear();

			Flow newFlow = null;
			if (packet.isFlagSYN() && packet.isFlagACK()) {
				// Backward
				newFlow = new Flow(
					currentInstanceTimestamp,
					bidirectional,
					packet,
					packet.getDst(),
					packet.getSrc(),
					packet.getDstPort(),
					packet.getSrcPort(),
					ACTIVITY_TIMEOUT
				);
				updateRetransmission(newFlow, packet);
            } else {
				// Forward
				newFlow = new Flow(
					currentInstanceTimestamp,	
					bidirectional,
					packet,
					ACTIVITY_TIMEOUT
				);
				updateRetransmission(newFlow, packet);
            }
			triggerTimer(newFlow, ctx);
			flowState.update(newFlow);
    	}
    }

	private void updateRetransmission(Flow flow, PacketInfo packet) throws Exception {
		if (packet.getProtocol() != ProtocolEnum.TCP) {
			return;
		}

		Integer hashCode = packet.getTcpRetransmission().hashCode();
		// If the element was successfully added to the hashset, then it has not been seen
		// before, and is not a retransmission.
		if (!tcpSeenState.contains(hashCode)) {
			tcpSeenState.put(hashCode, true);
		} else {
			if (Arrays.equals(flow.getSrc(), packet.getSrc())) {
				flow.setFwdTcpRetransCnt(flow.getFwdTcpRetransCnt() + 1);
			} else {
				flow.setBwdTcpRetransCnt(flow.getBwdTcpRetransCnt() + 1);
			}
		}
	}

	private void updateCummulativeDuration(Flow flow) {
		long curDuration = flow.getCummulativeConnectionDuration();
		curDuration += flow.getFlowLastSeen() - flow.getFlowStartTime();
		flow.setCummulativeConnectionDuration(curDuration);
	}

	private void triggerTimer(Flow flow, KeyedProcessFunction<String, PacketInfo, Flow>.Context ctx) throws Exception {
		long submitTriggerTime = ctx.timerService().currentProcessingTime() + (FLOW_TIMEOUT / 1000L) + 100; // 100 milliseconds grace
		ctx.timerService().registerProcessingTimeTimer(submitTriggerTime);
		long clearanceTriggerTime = ctx.timerService().currentProcessingTime() + 2 * (FLOW_TIMEOUT / 1000L) + 100; // 100 milliseconds grace
		ctx.timerService().registerProcessingTimeTimer(clearanceTriggerTime);
		flow.setTimerDeadline(submitTriggerTime);
		flow.setClearanceDeadline(clearanceTriggerTime);
	}

	private void untriggerSubmitTimer(Flow flow, KeyedProcessFunction<String, PacketInfo, Flow>.Context ctx) throws Exception {
		ctx.timerService().deleteProcessingTimeTimer(flow.getTimerDeadline());
	}

	private void untriggerClearanceTimer(Flow flow, KeyedProcessFunction<String, PacketInfo, Flow>.Context ctx) throws Exception {
		ctx.timerService().deleteProcessingTimeTimer(flow.getClearanceDeadline());
	}

	@Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Flow> out) throws Exception {
		Flow flow = flowState.value();
		if (flow == null) return;

		if (!flow.getSubmitted()) { // submit
			updateCummulativeDuration(flow);
			out.collect(flow);
			flow.setSubmitted(true);

			flowState.update(flow);
		} else { // src and dst history clearance
			tcpSeenState.clear();
			flowState.update(null);
		}
    }
}
