package id.ac.ui.cs.netlog.data.cicflowmeter;

import java.util.Arrays;

import id.ac.ui.cs.netlog.utils.DateUtils;
import id.ac.ui.cs.netlog.utils.PacketUtils;

public class TCPRetransmission {
    private byte[] src;
    private long seq;
    private long ack;
    private int payloadLength;

    private int window;

    private long timestamp;

    /** The timestamp is used for debugging purposes, and does not serve as equality criteria.
      * The current criteria seems sufficient for differentiating between retransmissions, but
      * may require expansion in the future.
     */
    public TCPRetransmission(byte[] src, long seq, long ack, int payloadLength, int window, long timestamp) {
        this.src = src;
        this.seq = seq;
        this.ack = ack;
        this.payloadLength = payloadLength;
        this.window = window;
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Arrays.hashCode(src);
        result = 31 * result + (int) (seq ^ (seq >>> 32));
        result = 31 * result + (int) (ack ^ (ack >>> 32));
        result = 31 * result + payloadLength;
        result = 31 * result + window;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof TCPRetransmission)) {
            return false;
        }
        TCPRetransmission tcpRetransmissionDTO = (TCPRetransmission) obj;
        return Arrays.equals(src, tcpRetransmissionDTO.src) &&
                seq == tcpRetransmissionDTO.seq &&
                ack == tcpRetransmissionDTO.ack &&
                payloadLength == tcpRetransmissionDTO.payloadLength &&
                window == tcpRetransmissionDTO.window;
    }
    @Override
    public String toString() {
        return "TcpRetransmissionDTO{" +
                "timestamp=" + DateUtils.convertEpochTimestamp2String(timestamp) +
                ", src=" + PacketUtils.byteArrayToIp(src) +
                ", seq=" + seq +
                ", ack=" + ack +
                ", payloadLength=" + payloadLength +
                ", window=" + window +
                '}';
    }
}
