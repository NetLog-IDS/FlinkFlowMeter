package id.ac.ui.cs.netlog.utils;

import java.util.Comparator;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;

public class ArrivalComparator implements Comparator<PacketInfo> {
    @Override
    public int compare(PacketInfo arg0, PacketInfo arg1) {
        if (arg0.getArrivalTime() > arg1.getArrivalTime()) {
            return 1;
        } else if (arg0.getArrivalTime() < arg1.getArrivalTime()) {
            return -1;
        }

        // Two elements cannot have same value in a set
        return arg0.getPacketId().compareTo(arg1.getPacketId());
    }
}
