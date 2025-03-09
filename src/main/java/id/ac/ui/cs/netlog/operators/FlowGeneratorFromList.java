package id.ac.ui.cs.netlog.operators;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.Flow;
import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.utils.TimeUtils;

public class FlowGeneratorFromList implements FlatMapFunction<List<PacketInfo>, Flow> {
    private static final Long FLOW_TIMEOUT = 120000000L;
    private static final Long ACTIVITY_TIMEOUT = 5000000L;

	private boolean bidirectional;
	
	public FlowGeneratorFromList(boolean bidirectional) {
		this.bidirectional = bidirectional;
	}

    @Override
    public void flatMap(List<PacketInfo> packets, Collector<Flow> out) throws Exception {
        Flow flow = null;
        for (PacketInfo packet : packets) {
			Long currentInstanceTimestamp = TimeUtils.getCurrentTimeMicro();
            if (flow != null) {
				Long currentTimestamp = packet.getTimeStamp();
	
				// Flow flow = currentFlows.get(id);
				if ((currentTimestamp - flow.getFlowStartTime()) > FLOW_TIMEOUT) {
					System.out.println(packet.getFlowBidirectionalId() + " TIMEOUTS");
					// Flow finished due flowtimeout: 
					// 1.- we move the flow to finished flow list
					// 2.- we eliminate the flow from the current flow list
					// 3.- we create a new flow with the packet-in-process
	
					if (flow.packetCount() > 1) out.collect(flow);
	
					flow = new Flow(
						currentInstanceTimestamp,
						bidirectional,
						packet,
						flow.getSrc(),
						flow.getDst(),
						flow.getSrcPort(),
						flow.getDstPort(),
						ACTIVITY_TIMEOUT
					);
				} else if (packet.isFlagFIN()) {
					// Flow finished due FIN flag (tcp only):
					// 1.- we add the packet-in-process to the flow (it is the last packet)
					// 2.- we move the flow to finished flow list
					// 3.- we eliminate the flow from the current flow list
					if (Arrays.equals(flow.getSrc(), packet.getSrc())) {
						// Forward Flow
	
						// How many forward FIN received?
						if (flow.setFwdFINFlags() == 1) {
							// Flow finished due FIN flag (tcp only)?:
							// 1.- we add the packet-in-process to the flow (it is the last packet)
							// 2.- we move the flow to finished flow list
							// 3.- we eliminate the flow from the current flow list
							// TODO: BUG HERE, should be Fwd >= 1 and Bwd >= 1
							if ((flow.getBwdFINFlags() + flow.getBwdFINFlags()) == 2) {
								System.out.println(packet.getFlowBidirectionalId() + " FWD 2 TRUE");
								// Forward Flow Finished.
								flow.addPacket(packet);
								out.collect(flow);
								flow = null;
							} else {
								System.out.println(packet.getFlowBidirectionalId() + " FWD 2 ELSE");
								flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
								flow.addPacket(packet);
							}
						} else {
							// Some Error
							System.out.println(packet.getFlowBidirectionalId() + " FWD SOME ERROR");
						}
					} else {
						// Backward Flow
	
						// How many backward FIN packets received?
						if (flow.setBwdFINFlags() == 1) {
							// Flow finished due FIN flag (tcp only)?:
							// 1.- we add the packet-in-process to the flow (it is the last packet)
							// 2.- we move the flow to finished flow list
							// 3.- we eliminate the flow from the current flow list
							// TODO: BUG HERE, should be Fwd >= 1 and Bwd >= 1
							if ((flow.getBwdFINFlags() + flow.getBwdFINFlags()) == 2) {
								System.out.println(packet.getFlowBidirectionalId() + " BWD 2 TRUE");
								// Backward Flow Finished.
								flow.addPacket(packet);
								out.collect(flow);
								flow = null;
							} else {
								System.out.println(packet.getFlowBidirectionalId() + " BWD 2 ELSE");
								flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
								flow.addPacket(packet);
							}
						} else {
							// Some Error
							System.out.println(packet.getFlowBidirectionalId() + " BWD SOME ERROR");
						}    				
					}               
				}else if(packet.isFlagRST()) {
					System.out.println("RST");
					// Flow finished due RST flag (tcp only):
					// 1.- we add the packet-in-process to the flow (it is the last packet)
					// 2.- we move the flow to finished flow list
					// 3.- we eliminate the flow from the current flow list 
	
					flow.addPacket(packet);
					out.collect(flow);
					flow = null;
				}else{
					if (Arrays.equals(flow.getSrc(), packet.getSrc()) && (flow.getFwdFINFlags() == 0)) {
						System.out.println(packet.getFlowBidirectionalId() + " FWD UWOO");
						// Forward Flow and fwdFIN = 0
						flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
						flow.addPacket(packet);
					} else if (flow.getBwdFINFlags() == 0) {
						// TODO: possible BUG HERE, Arrays.equals(flow src, packet dst) && bwdfinflags == 0
						System.out.println(packet.getFlowBidirectionalId() + " BWD UWOO");
						// Backward Flow and bwdFIN = 0
						flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
						flow.addPacket(packet);
					} else {
						// FLOW already closed!!!
						System.out.println(packet.getFlowBidirectionalId() + " CLOSEEE");
					}
				}
			} else {
				System.out.println(packet.getFlowBidirectionalId() + " SIIEEEE");
				// TODO: make fwd and bwd have same key
				flow = new Flow(
					currentInstanceTimestamp,
					bidirectional,
					packet,
					ACTIVITY_TIMEOUT
				);
			}	
        }

		if (flow != null && flow.packetCount() > 1) {
			out.collect(flow);
		}
    }    
}
