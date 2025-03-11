package id.ac.ui.cs.netlog.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
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
		System.out.println("[PROCESSING_PACKET] " + packet.getFlowId() + " " + packet.getOrder());

		initializeDefaultValue();

		OrderProcessingState state = processingState.value();

		Long submittedOrder = state.getSubmittedOrder();
		if (packet.getOrder() <= submittedOrder) {
			System.out.println("[PACKET_IGNORED] Because order is less than submitted order");
			return;
		}

		addPacketToCollections(packet, state);
		while (true) if (!submitIfCompleted(state, ctx, out)) break;
		addTimeoutIfNeeded(state, ctx);

		processingState.update(state);
    }

	@Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<PacketInfo>> out) throws Exception {
		System.out.println("[STARTING TIMER JOB...]");

		OrderProcessingState state = processingState.value();
		state.setTimerTimestamp(null);
		Long lastOrder = getEarliestSubmittableOrder(state);

		if (lastOrder != null) {
			PacketInfo comparator = PacketInfo.getOrderComparator(lastOrder);
			NavigableSet<PacketInfo> submittingPackets = state.getPacketSet().headSet(comparator, true);

			state.setSubmittedOrder(lastOrder);
			List<PacketInfo> submittedList = new ArrayList<>();
			for (PacketInfo packet : submittingPackets) {
				submittedList.add(packet);
			}
			
			for (PacketInfo packet : submittedList) {
				state.getPacketSet().remove(packet);
				state.getPacketArrival().remove(packet);
				state.getPacketTimestamp().remove(packet);
			}
			out.collect(submittedList);
		} else {
			// TODO: choose whether to discard or just submit -> we need to support UDP
			PacketInfo firstPacket = state.getPacketTimestamp().first();
			PacketInfo comparator = PacketInfo.getTimestampComparatorUpperBound(firstPacket.getTimeStamp() + FLOW_TIMEOUT);
			NavigableSet<PacketInfo> submittingPackets = state.getPacketTimestamp().headSet(comparator, false);
			PacketInfo lastPacket = submittingPackets.last();

			state.setSubmittedOrder(lastPacket.getOrder());
			List<PacketInfo> submittedList = new ArrayList<>();
			for (PacketInfo packet : submittingPackets) {
				submittedList.add(packet);
			}

			for (PacketInfo packet : submittedList) {
				state.getPacketSet().remove(packet);
				state.getPacketArrival().remove(packet);
				state.getPacketTimestamp().remove(packet);
			}
			out.collect(submittedList);

			// TODO: If clear
			// PacketInfo lastPacket = state.getPacketSet().last();
			// state.setSubmittedOrder(lastPacket.getOrder());
			// state.getPacketSet().clear();
			// state.getPacketArrival().clear();
			// state.getPacketTimestamp().clear();
			// state.getFinFwdQueue().clear();
			// state.getFinBwdQueue().clear();
			// state.getRstQueue().clear();
		}
		
		while (true) if (!submitIfCompleted(state, ctx, out)) break;
		addTimeoutIfNeeded(state, ctx);

		processingState.update(state);
    }

	private void addPacketToCollections(PacketInfo packet,OrderProcessingState state) {
		state.getPacketSet().add(packet);
		state.getPacketArrival().add(packet);
		state.getPacketTimestamp().add(packet);
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
			Long packetCount = lastOrder - state.getSubmittedOrder();
			
			NavigableSet<PacketInfo> submittingPackets = state.getPacketSet().headSet(comparator, true);
			if (submittingPackets.size() == packetCount) {
				state.setSubmittedOrder(lastOrder);
				List<PacketInfo> submittedList = new ArrayList<>();
				for (PacketInfo packet : submittingPackets) {
					submittedList.add(packet);
				}
				for (PacketInfo packet : submittedList) {
					state.getPacketSet().remove(packet);
					state.getPacketArrival().remove(packet);
					state.getPacketTimestamp().remove(packet);
				}
				out.collect(submittedList);
				ctx.timerService().deleteProcessingTimeTimer(state.getTimerTimestamp());
				state.setTimerTimestamp(null);
				isSubmitted = Boolean.TRUE;
			}
		}

		return isSubmitted;
	}

	private Long getEarliestSubmittableOrder(OrderProcessingState state) throws Exception {
		Long finOrder = getOrderOfEarliestFinPair(state);
		Long rstOrder = getOrderOfEarliestRst(state);

		if (finOrder == null) {
			return rstOrder;
		} else {
			if (rstOrder == null) {
				return finOrder;
			} else {
				return Math.min(finOrder, rstOrder);
			}
		}
	}

	private void addTimeoutIfNeeded(
		OrderProcessingState state,
		KeyedProcessFunction<String, PacketInfo, List<PacketInfo>>.Context ctx
	) throws Exception {
		if (state.getPacketSet().size() == 0 || state.getTimerTimestamp() != null) return;

		PacketInfo earliestPacket;
		try	{
			earliestPacket = state.getPacketArrival().first();
		} catch (NoSuchElementException exception) {
			earliestPacket = null;
		}

		Long diff = Math.max((FLOW_TIMEOUT / 1000L) - earliestPacket.getArrivalTime(), 0);
		long triggerTime = ctx.timerService().currentProcessingTime() + diff;

		ctx.timerService().registerProcessingTimeTimer(triggerTime);
		state.setTimerTimestamp(triggerTime);
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
