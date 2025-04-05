package id.ac.ui.cs.netlog.data.cicflowmeter;

public enum ProtocolEnum {
    DEFAULT(0),
    ICMP(1),
    IGMP(2),
    TCP(6),
    UDP(17),
    SCTP(132);

    public final int val;

    ProtocolEnum(int num) {
        this.val = num;
    }
}
