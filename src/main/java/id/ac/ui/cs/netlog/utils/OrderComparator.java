package id.ac.ui.cs.netlog.utils;

import java.util.Comparator;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;

public class OrderComparator implements Comparator<PacketInfo> {
    @Override
    public int compare(PacketInfo arg0, PacketInfo arg1) {
        if (arg0.getOrder() > arg1.getOrder()) {
            return 1;
        } else if (arg0.getOrder() < arg1.getOrder()) {
            return -1;
        } else {
            return 0;
        }
    }
}
