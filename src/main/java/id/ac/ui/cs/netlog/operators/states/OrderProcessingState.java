package id.ac.ui.cs.netlog.operators.states;

import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import lombok.Data;

@Data
public class OrderProcessingState {
    private NavigableSet<PacketInfo> packetSet;
    private Long submittedOrder;
    private Queue<Long> finFwdQueue;
    private Queue<Long> finBwdQueue;
    private Queue<Long> rstQueue;
    private Boolean hasTimer;

    public OrderProcessingState() {
        packetSet = new TreeSet<>();
		submittedOrder = 0L;
		finFwdQueue = new PriorityQueue<>();
        finBwdQueue = new PriorityQueue<>();
        hasTimer = Boolean.FALSE;
    }
}
