package id.ac.ui.cs.netlog.operators.states;

import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.utils.ArrivalComparator;
import id.ac.ui.cs.netlog.utils.OrderComparator;
import id.ac.ui.cs.netlog.utils.TimestampComparator;
import lombok.Data;

@Data
public class OrderProcessingState {
    private NavigableSet<PacketInfo> packetSet;
    private NavigableSet<PacketInfo> packetArrival;
    private NavigableSet<PacketInfo> packetTimestamp;
    private Long submittedOrder;
    private Queue<Long> finFwdQueue;
    private Queue<Long> finBwdQueue;
    private Queue<Long> rstQueue;
    private Long timerTimestamp;

    public OrderProcessingState() {
        packetSet = new TreeSet<>(new OrderComparator());
        packetArrival = new TreeSet<>(new ArrivalComparator());
        packetTimestamp = new TreeSet<>(new TimestampComparator());
		submittedOrder = 0L;
		finFwdQueue = new PriorityQueue<>();
        finBwdQueue = new PriorityQueue<>();
        rstQueue = new PriorityQueue<>();
        timerTimestamp = null;
    }
}
