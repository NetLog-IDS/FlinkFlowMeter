package id.ac.ui.cs.netlog.operators;

import java.util.Arrays;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.Flow;
import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
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

		System.out.println("=== PACKET START FOR " + packet.getFlowBidirectionalId() + " ===");

		Flow flow = flowState.value();
		Long currentInstanceTimestamp = TimeUtils.getCurrentTimeMicro();
		// System.out.println("CURRENT TIME: " + currentInstanceTimestamp.toString());
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

				flowState.update(new Flow(
					currentInstanceTimestamp,
					bidirectional,
					packet,
					flow.getSrc(),
					flow.getDst(),
					flow.getSrcPort(),
					flow.getDstPort(),
					ACTIVITY_TIMEOUT
				));
				System.out.println(packet.getFlowBidirectionalId()+ " TRIGGERING TIMER SERVICE");
				long triggerTime = ctx.timerService().currentProcessingTime() + (FLOW_TIMEOUT / 1000L);
				ctx.timerService().registerProcessingTimeTimer(triggerTime);
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
    					if ((flow.getBwdFINFlags() + flow.getBwdFINFlags()) == 2) {
							System.out.println(packet.getFlowBidirectionalId() + " FWD 2 TRUE");
                            // Forward Flow Finished.
    		    	    	flow.addPacket(packet);
                            out.collect(flow);
							flowState.update(null);
    					} else {
							System.out.println(packet.getFlowBidirectionalId() + " FWD 2 ELSE");
    		    			flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
    		    			flow.addPacket(packet);
							flowState.update(flow);
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
    					if ((flow.getBwdFINFlags() + flow.getBwdFINFlags()) == 2) {
							System.out.println(packet.getFlowBidirectionalId() + " BWD 2 TRUE");
                            // Backward Flow Finished.
    		    	    	flow.addPacket(packet);
    		                out.collect(flow);
							flowState.update(null);
    					} else {
							System.out.println(packet.getFlowBidirectionalId() + " BWD 2 ELSE");
    		    			flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
    		    			flow.addPacket(packet);
							flowState.update(flow);
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
				flowState.update(null);
    		}else{
    			if (Arrays.equals(flow.getSrc(), packet.getSrc()) && (flow.getFwdFINFlags() == 0)) {
					System.out.println(packet.getFlowBidirectionalId() + " FWD UWOO");
                    // Forward Flow and fwdFIN = 0
        			flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
        			flow.addPacket(packet);
					flowState.update(flow);
    			} else if (flow.getBwdFINFlags() == 0) {
					System.out.println(packet.getFlowBidirectionalId() + " BWD UWOO");
    			    // Backward Flow and bwdFIN = 0
        			flow.updateActiveIdleTime(currentTimestamp, ACTIVITY_TIMEOUT);
        			flow.addPacket(packet);
					flowState.update(flow);
    			} else {
        		    // FLOW already closed!!!
					System.out.println(packet.getFlowBidirectionalId() + " CLOSEEE");
    			}
    		}
    	} else {
			System.out.println(packet.getFlowBidirectionalId() + " SIIEEEE");
			// TODO: make fwd and bwd have same key
			flowState.update(new Flow(
				currentInstanceTimestamp,
				bidirectional,
				packet,
				ACTIVITY_TIMEOUT
			));
			System.out.println(packet.getFlowBidirectionalId()+ " TRIGGERING TIMER SERVICE");
			long triggerTime = ctx.timerService().currentProcessingTime() + (FLOW_TIMEOUT / 1000L);
			ctx.timerService().registerProcessingTimeTimer(triggerTime);
    	}

		System.out.println("=== PACKET ENDED FOR " + packet.getFlowBidirectionalId() + " ===");
    }

	// TODO: there's concern about unsynchronized time which makes timestamp - flow.getFlowStartTime() < 0
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Flow> out) throws Exception {
		// Long timestampMicro = timestamp * 1000L;

		System.out.println("==TIME TRIGGER START");
		Flow flow = flowState.value();
		if (flow == null) return;

		Long processStarTimeMilli = flow.getProcessStartTime() / 1000L;

		System.out.println(((Long) timestamp).toString() + " - " + processStarTimeMilli);

		// Flow finished due flowtimeout: 
		// 1.- we move the flow to finished flow list
		// 2.- we eliminate the flow from the current flow list
		// 3.- we create a new flow with the packet-in-process
		if ((timestamp - processStarTimeMilli) >= (FLOW_TIMEOUT / 1000L)) {
			System.out.println(flow.packetCount());
			if (flow.packetCount() > 1) {
				System.out.println("COLLECTED");
				out.collect(flow);
			}
			flowState.update(null);
		}

		System.out.println("==TIME TRIGGER END");
    }
}
