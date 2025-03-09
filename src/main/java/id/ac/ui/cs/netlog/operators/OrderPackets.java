package id.ac.ui.cs.netlog.operators;

import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.utils.PacketUtils;

public class OrderPackets extends KeyedProcessFunction<String, PacketInfo, List<PacketInfo>> {
    private static final Long FLOW_TIMEOUT = 120000000L;

	// TODO: compare with MapState, etc
	private transient ValueState<NavigableSet<PacketInfo>> packetCollectionState;
	private transient ValueState<Long> submittedOrderState;
	private transient ValueState<Queue<Long>> finFwdCollectionState;
	private transient ValueState<Queue<Long>> finBwdCollectionState;
	private transient ValueState<Queue<Long>> rstCollectionState;
	private transient ValueState<Boolean> hasTimerState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<NavigableSet<PacketInfo>> packetCollectionDescriptor = new ValueStateDescriptor<>(
			"packetCollectionState",
			TypeInformation.of(new TypeHint<NavigableSet<PacketInfo>>() {})
		);
		ValueStateDescriptor<Long> submittedOrderDescriptor = new ValueStateDescriptor<>(
			"submittedOrderState",
			TypeInformation.of(new TypeHint<Long>() {})
		);
		ValueStateDescriptor<Queue<Long>> finFwdCollectionDescriptor = new ValueStateDescriptor<>(
			"finFwdCollectionState",
			TypeInformation.of(new TypeHint<Queue<Long>>() {})
		);
		ValueStateDescriptor<Queue<Long>> finBwdCollectionDescriptor = new ValueStateDescriptor<>(
			"finBwdCollectionState",
			TypeInformation.of(new TypeHint<Queue<Long>>() {})
		);
		ValueStateDescriptor<Queue<Long>> rstCollectionDescriptor = new ValueStateDescriptor<>(
			"rstCollectionState",
			TypeInformation.of(new TypeHint<Queue<Long>>() {})
		);
		ValueStateDescriptor<Boolean> hasTimerDescriptor = new ValueStateDescriptor<>(
			"hasTimerState",
			TypeInformation.of(new TypeHint<Boolean>() {})
		);

		packetCollectionState = getRuntimeContext().getState(packetCollectionDescriptor);
		submittedOrderState = getRuntimeContext().getState(submittedOrderDescriptor);
		finFwdCollectionState = getRuntimeContext().getState(finFwdCollectionDescriptor);
		finBwdCollectionState = getRuntimeContext().getState(finBwdCollectionDescriptor);
		rstCollectionState = getRuntimeContext().getState(rstCollectionDescriptor);
		hasTimerState = getRuntimeContext().getState(hasTimerDescriptor);
	}

	private void initializeDefaultValue() throws Exception {
		if (packetCollectionState.value() == null) packetCollectionState.update(new TreeSet<>());
		if (submittedOrderState.value() == null) submittedOrderState.update(0L);
		if (finFwdCollectionState.value() == null) finFwdCollectionState.update(new PriorityQueue<>());
		if (finBwdCollectionState.value() == null) finBwdCollectionState.update(new PriorityQueue<>());
		if (rstCollectionState.value() == null) rstCollectionState.update(new PriorityQueue<>());
		if (hasTimerState.value() == null) hasTimerState.update(false);
	}

    @Override
    public void processElement(PacketInfo packet, KeyedProcessFunction<String, PacketInfo, List<PacketInfo>>.Context ctx,
            Collector<List<PacketInfo>> out) throws Exception {
		initializeDefaultValue();

		Long submittedOrder = submittedOrderState.value();
		if (packet.getOrder() <= submittedOrder) return;

		NavigableSet<PacketInfo> packetSet = packetCollectionState.value();
		Queue<Long> finFwdQueue = finFwdCollectionState.value();
		Queue<Long> finBwdQueue = finBwdCollectionState.value();
		Queue<Long> rstQueue = rstCollectionState.value();

		addPacketToCollections(packet, packetSet, finFwdQueue, finBwdQueue, rstQueue);
		submitUntilOrder(submittedOrder, packetSet, finFwdQueue, finBwdQueue, rstQueue);
		addTimeoutIfNeeded(ctx);

		// TODO: update DSes, excluding submittedOrder and hasTimer since they are primitives -> do it on assigning functions
		packetCollectionState.update(packetSet);
		finFwdCollectionState.update(finFwdQueue);
		finBwdCollectionState.update(finBwdQueue);
		rstCollectionState.update(rstQueue);
    }

	// TODO: submit must reset hasTimer
	private void submitPackets() throws Exception {

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

	private void addPacketToCollections(
		PacketInfo packet,
		NavigableSet<PacketInfo> packetSet,
		Queue<Long> finFwdQueue,
		Queue<Long> finBwdQueue,
		Queue<Long> rstQueue
	) {
		packetSet.add(packet);
		if (packet.isFlagFIN()) {
			Queue<Long> queue = finBwdQueue;
			if (PacketUtils.ipLesserThan(packet.getSrc(), packet.getDst())) queue = finFwdQueue;

			queue.add(packet.getOrder());
		} else if (packet.isFlagRST()) {
			rstQueue.add(packet.getOrder());
		}
	}

	private Long getEarliestSubmittableOrder(
		Long submittedOrder,
		Queue<Long> finFwdQueue,
		Queue<Long> finBwdQueue,
		Queue<Long> rstQueue
	) throws Exception {
		Long finOrder = getOrderOfEarliestFinPair(submittedOrder, finFwdQueue, finBwdQueue);
		Long rstOrder = getOrderOfEarliestRst(submittedOrder, rstQueue);

		return (finOrder == null) ? rstOrder : 
			(rstOrder == null ? finOrder : Math.min(finOrder, rstOrder));
	}

	private void submitUntilOrder(
		Long submittedOrder,
		NavigableSet<PacketInfo> packetSet,
		Queue<Long> finFwdQueue,
		Queue<Long> finBwdQueue,
		Queue<Long> rstQueue
	) throws Exception {
		Long lastOrder = getEarliestSubmittableOrder(submittedOrder, finFwdQueue, finBwdQueue, rstQueue);
		if (lastOrder != null) {
			PacketInfo comparator = PacketInfo.getOrderComparator(lastOrder);
			Long packetCount = lastOrder - submittedOrder + 1;
			
			NavigableSet<PacketInfo> submittingPackets = packetSet.headSet(comparator, true);
			if (submittingPackets.size() == packetCount) {
				submitPackets();
			}
		}
	}

	private void addTimeoutIfNeeded(
		KeyedProcessFunction<String, PacketInfo, List<PacketInfo>>.Context ctx
	) throws Exception {
		if (hasTimerState.value()) return;

		long triggerTime = ctx.timerService().currentProcessingTime() + (FLOW_TIMEOUT / 1000L);
		ctx.timerService().registerProcessingTimeTimer(triggerTime);
		hasTimerState.update(true);
	}

	private Long getOrderOfEarliestFinPair(Long submittedOrder, Queue<Long> finFwdQueue, Queue<Long> finBwdQueue) throws Exception {
		Long firstOrder = submittedOrder + 1;

		clearQueue(finFwdQueue, firstOrder);
		Long firstFwd = finFwdQueue.peek();

		clearQueue(finBwdQueue, firstOrder);
		Long firstBwd = finBwdQueue.peek();

		if (firstFwd == null || firstBwd == null) return null;
		// return Math.max(firstFwd, firstBwd) - firstOrder + 1;
		return Math.max(firstFwd, firstBwd);
	}

	private Long getOrderOfEarliestRst(Long submittedOrder, Queue<Long> rstQueue) throws Exception {
		Long firstOrder = submittedOrder + 1;

		clearQueue(rstQueue, firstOrder);
		return rstQueue.peek();
	}

	private void clearQueue(Queue<Long> queue, Long firstOrder) {
		while (queue.peek() != null && queue.peek() < firstOrder) {
			queue.poll();
		}
	}

	
}
