package id.ac.ui.cs.netlog.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Queue;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.operators.states.OrderProcessingState;
import id.ac.ui.cs.netlog.utils.PacketUtils;

public class OrderPackets extends KeyedProcessFunction<String, PacketInfo, List<PacketInfo>> {
    private static final Long FLOW_TIMEOUT = 120000000L;

	// TODO: compare with MapState, etc
	private transient ValueState<OrderProcessingState> processingState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<OrderProcessingState> processingDescriptor = new ValueStateDescriptor<>(
			"processingState",
			TypeInformation.of(new TypeHint<OrderProcessingState>() {})
		);
		processingState = getRuntimeContext().getState(processingDescriptor);
	}

	private void initializeDefaultValue() throws Exception {
		if (processingState.value() == null) processingState.update(new OrderProcessingState());
	}

    @Override
    public void processElement(PacketInfo packet, KeyedProcessFunction<String, PacketInfo, List<PacketInfo>>.Context ctx,
            Collector<List<PacketInfo>> out) throws Exception {
		initializeDefaultValue();

		OrderProcessingState state = processingState.value();

		Long submittedOrder = state.getSubmittedOrder();
		if (packet.getOrder() <= submittedOrder) return;

		addPacketToCollections(packet, state);
		while (true) if (!submitIfCompleted(state, ctx, out)) break;
		addTimeoutIfNeeded(state, ctx);

		processingState.update(state);
    }

	@Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<PacketInfo>> out) throws Exception {
		// Long timestampMicro = timestamp * 1000L;

		// System.out.println("==TIME TRIGGER START");
		// Flow flow = flowState.value();
		// if (flow == null) return;

		// Long processStarTimeMilli = flow.getProcessStartTime() / 1000L;

		// System.out.println(((Long) timestamp).toString() + " - " + processStarTimeMilli);

		// // Flow finished due flowtimeout: 
		// // 1.- we move the flow to finished flow list
		// // 2.- we eliminate the flow from the current flow list
		// // 3.- we create a new flow with the packet-in-process
		// if ((timestamp - processStarTimeMilli) >= (FLOW_TIMEOUT / 1000L)) {
		// 	System.out.println(flow.packetCount());
		// 	if (flow.packetCount() > 1) {
		// 		System.out.println("COLLECTED");
		// 		out.collect(flow);
		// 	}
		// 	flowState.update(null);
		// }

		// System.out.println("==TIME TRIGGER END");
    }

	private void addPacketToCollections(PacketInfo packet,OrderProcessingState state) {
		state.getPacketSet().add(packet);
		if (packet.isFlagFIN()) {
			Queue<Long> queue = state.getFinBwdQueue();
			if (PacketUtils.ipLesserThan(packet.getSrc(), packet.getDst())) queue = state.getFinFwdQueue();

			queue.add(packet.getOrder());
		} else if (packet.isFlagRST()) {
			state.getRstQueue().add(packet.getOrder());
		}
	}

	// TODO: submit must reset hasTimer
	private Boolean submitIfCompleted(OrderProcessingState state, KeyedProcessFunction<String, PacketInfo, List<PacketInfo>>.Context ctx, Collector<List<PacketInfo>> out) throws Exception {
		Boolean isSubmitted = Boolean.FALSE;
		Long lastOrder = getEarliestSubmittableOrder(state);

		if (lastOrder != null) {
			PacketInfo comparator = PacketInfo.getOrderComparator(lastOrder);
			Long packetCount = lastOrder - state.getSubmittedOrder() + 1;
			
			NavigableSet<PacketInfo> submittingPackets = state.getPacketSet().headSet(comparator, true);
			if (submittingPackets.size() == packetCount) {
				state.setSubmittedOrder(lastOrder);
				List<PacketInfo> submittedList = new ArrayList<>();
				for (PacketInfo packet : submittingPackets) {
					submittedList.add(packet);
					state.getPacketSet().remove(packet);
					state.getPacketArrival().remove(packet);
				}
				out.collect(submittedList);
				isSubmitted = Boolean.TRUE;
			}
		}

		return isSubmitted;
	}

	private Long getEarliestSubmittableOrder(OrderProcessingState state) throws Exception {
		Long finOrder = getOrderOfEarliestFinPair(state);
		Long rstOrder = getOrderOfEarliestRst(state);

		return (finOrder == null) ? rstOrder : 
			(rstOrder == null ? finOrder : Math.min(finOrder, rstOrder));
	}

	private void addTimeoutIfNeeded(
		OrderProcessingState state,
		KeyedProcessFunction<String, PacketInfo, List<PacketInfo>>.Context ctx
	) throws Exception {
		if (state.getTimerStartOrder() >= state.getSubmittedOrder() + 1) return;

		PacketInfo earliestPacket = state.getPacketArrival().first();
		Long diff = Math.max((FLOW_TIMEOUT / 1000L) - earliestPacket.getArrivalTime(), 0);
		long triggerTime = ctx.timerService().currentProcessingTime() + diff;

		ctx.timerService().registerProcessingTimeTimer(triggerTime);
		state.setTimerStartOrder(state.getSubmittedOrder() + 1);
	}

	private Long getOrderOfEarliestFinPair(OrderProcessingState state) throws Exception {
		Long firstOrder = state.getSubmittedOrder() + 1;

		clearQueue(state.getFinFwdQueue(), firstOrder);
		Long firstFwd = state.getFinFwdQueue().peek();

		clearQueue(state.getFinBwdQueue(), firstOrder);
		Long firstBwd = state.getFinBwdQueue().peek();

		if (firstFwd == null || firstBwd == null) return null;
		return Math.max(firstFwd, firstBwd);
	}

	private Long getOrderOfEarliestRst(OrderProcessingState state) throws Exception {
		Long firstOrder = state.getSubmittedOrder() + 1;

		clearQueue(state.getRstQueue(), firstOrder);
		return state.getRstQueue().peek();
	}

	private void clearQueue(Queue<Long> queue, Long firstOrder) {
		while (queue.peek() != null && queue.peek() < firstOrder) {
			queue.poll();
		}
	}
}
