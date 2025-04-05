package id.ac.ui.cs.netlog.data.cicflowmeter;

import id.ac.ui.cs.netlog.utils.DateUtils;
import id.ac.ui.cs.netlog.utils.PacketUtils;

import lombok.Data;

@Data
public class FlowStats {
    // Flow Identifiers
    private String fid;             // Flow ID
    private String srcIp;           // Source IP
    private int srcPort;           // Source Port
    private String dstIp;           // Destination IP
    private int dstPort;           // Destination Port
    private int protocol;          // Protocol
    private String timestamp;       // Timestamp
    
    // Basic Flow Statistics
    private double flowDuration;    // Flow Duration
    private long totalFwdPackets;   // Total Forward Packets
    private long totalBwdPackets;   // Total Backward Packets
    private double totalFwdLength;    // Total Length of Forward Packets
    private double totalBwdLength;    // Total Length of Backward Packets
    
    // Packet Length
    private double fwdPacketLengthMax;    // Forward Packet Length Max
    private double fwdPacketLengthMin;    // Forward Packet Length Min
    private double fwdPacketLengthMean; // Forward Packet Length Mean
    private double fwdPacketLengthStd;  // Forward Packet Length Standard Deviation
    private double bwdPacketLengthMax;    // Backward Packet Length Max
    private double bwdPacketLengthMin;    // Backward Packet Length Min
    private double bwdPacketLengthMean; // Backward Packet Length Mean
    private double bwdPacketLengthStd;  // Backward Packet Length Standard Deviation
    
    // Flow Rate
    private double flowBytesPerSec;     // Flow Bytes per Second
    private double flowPacketsPerSec;   // Flow Packets per Second
    
    // IAT (Inter Arrival Time) Statistics
    private double flowIatMean;         // Flow IAT Mean
    private double flowIatStd;          // Flow IAT Standard Deviation
    private double flowIatMax;          // Flow IAT Max
    private double flowIatMin;          // Flow IAT Min
    private double fwdIatTotal;         // Forward IAT Total
    private double fwdIatMean;          // Forward IAT Mean
    private double fwdIatStd;           // Forward IAT Standard Deviation
    private double fwdIatMax;           // Forward IAT Max
    private double fwdIatMin;           // Forward IAT Min
    private double bwdIatTotal;         // Backward IAT Total
    private double bwdIatMean;          // Backward IAT Mean
    private double bwdIatStd;           // Backward IAT Standard Deviation
    private double bwdIatMax;           // Backward IAT Max
    private double bwdIatMin;           // Backward IAT Min
    
    // PSH, URG, RST Flags
    private int fwdPshFlags;            // Forward PSH Flags
    private int bwdPshFlags;            // Backward PSH Flags
    private int fwdUrgFlags;            // Forward URG Flags
    private int bwdUrgFlags;            // Backward URG Flags
    private int fwdRstFlags;            // Forward RST Flags
    private int bwdRstFlags;            // Backward RST Flags
    
    // Header Lengths
    private long fwdHeaderLength;       // Forward Header Length
    private long bwdHeaderLength;       // Backward Header Length
    
    // Packet Rate
    private double fwdPacketsPerSec;    // Forward Packets per Second
    private double bwdPacketsPerSec;    // Backward Packets per Second
    
    // Total Packet Length
    private double packetLengthMin;       // Packet Length Min
    private double packetLengthMax;       // Packet Length Max
    private double packetLengthMean;    // Packet Length Mean
    private double packetLengthStd;     // Packet Length Standard Deviation
    private double packetLengthVar;     // Packet Length Variance
    
    // TCP Flag Counts
    private int finCount;               // FIN Flag Count
    private int synCount;               // SYN Flag Count
    private int rstCount;               // RST Flag Count
    private int pshCount;               // PSH Flag Count
    private int ackCount;               // ACK Flag Count
    private int urgCount;               // URG Flag Count
    private int cwrCount;               // CWR Flag Count
    private int eceCount;               // ECE Flag Count
    
    // Misc
    private double downUpRatio;         // Down/Up Ratio
    private double avgPacketSize;       // Average Packet Size
    private double fwdSegmentSizeAvg;   // Forward Segment Size Average
    private double bwdSegmentSizeAvg;   // Backward Segment Size Average
    
    // Bulk
    private double fwdBytesPerBulkAvg;  // Forward Bytes per Bulk Average
    private double fwdPacketsPerBulkAvg; // Forward Packets per Bulk Average
    private double fwdBulkRateAvg;      // Forward Bulk Rate Average
    private double bwdBytesPerBulkAvg;  // Backward Bytes per Bulk Average
    private double bwdPacketsPerBulkAvg; // Backward Packets per Bulk Average
    private double bwdBulkRateAvg;      // Backward Bulk Rate Average
    
    // Subflow
    private double subflowFwdPackets;     // Subflow Forward Packets
    private double subflowFwdBytes;       // Subflow Forward Bytes
    private double subflowBwdPackets;     // Subflow Backward Packets
    private double subflowBwdBytes;       // Subflow Backward Bytes
    
    // Window Statistics
    private long fwdInitWinBytes;       // Forward Initial Window Bytes
    private long bwdInitWinBytes;       // Backward Initial Window Bytes
    private long fwdActDataPackets;     // Forward Active Data Packets
    private long bwdActDataPackets;     // Backward Active Data Packets
    private long fwdSegSizeMin;         // Forward Segment Size Min
    private long bwdSegSizeMin;         // Backward Segment Size Min
    
    // Active/Idle Statistics
    private double activeMean;          // Active Mean
    private double activeStd;           // Active Standard Deviation
    private double activeMax;           // Active Max
    private double activeMin;           // Active Min
    private double idleMean;            // Idle Mean
    private double idleStd;             // Idle Standard Deviation
    private double idleMax;             // Idle Max
    private double idleMin;             // Idle Min

    // ICMP
    private int icmpCode;
    private int icmpType;

    // Retransmission
    private int fwdTCPRetransCount;
    private int bwdTCPRetransCount;
    private int totalTCPRetransCount;

    // Cummulative
    private long cummConnectionTime;
    
    // Classification
    private String label;               // Traffic Classification Label

    public FlowStats(Flow flow) {
        // TODO: check yang Max Min apa boleh double atau malah jgn

        // Basic flow identifiers
        this.fid = flow.getFlowId();
        this.srcIp = PacketUtils.byteArrayToIp(flow.getSrc());
        this.srcPort = flow.getSrcPort();
        this.dstIp = PacketUtils.byteArrayToIp(flow.getDst());
        this.dstPort = flow.getDstPort();
        this.protocol = flow.getProtocol().val;
        
        // Timestamp
        this.timestamp = DateUtils.convertEpochTimestamp2String(flow.getFlowStartTime());
        
        // Flow duration
        this.flowDuration = flow.getFlowLastSeen() - flow.getFlowStartTime();

        this.totalFwdPackets = flow.getFwdPktStats().getN();
        this.totalBwdPackets = flow.getBwdPktStats().getN();
        this.totalFwdLength = flow.getFwdPktStats().getSum(); // TODO: check double or not
        this.totalBwdLength = flow.getBwdPktStats().getSum(); // TODO: check double or not
        
        // Packet statistics
        // Forward packets
        if (flow.getFwdPktStats().getN() > 0L) {
            this.fwdPacketLengthMax = flow.getFwdPktStats().getMax(); // TODO: check double or not
            this.fwdPacketLengthMin = flow.getFwdPktStats().getMin(); // TODO: check double or not
            this.fwdPacketLengthMean = flow.getFwdPktStats().getMean();
            this.fwdPacketLengthStd = flow.getFwdPktStats().getStandardDeviation();
        }
        
        // Backward packets
        if (flow.getBwdPktStats().getN() > 0L) {
            this.bwdPacketLengthMax = flow.getBwdPktStats().getMax(); // TODO: check double or not
            this.bwdPacketLengthMin = flow.getBwdPktStats().getMin(); // TODO: check double or not
            this.bwdPacketLengthMean = flow.getBwdPktStats().getMean();
            this.bwdPacketLengthStd = flow.getBwdPktStats().getStandardDeviation();
        }

        // Flow rates
        if (flowDuration != 0) {
            this.flowBytesPerSec = ((double) (flow.getForwardBytes() + flow.getBackwardBytes())) / ((double) flowDuration / 1000000L);
            this.flowPacketsPerSec = ((double) flow.packetCount()) / ((double) flowDuration / 1000000L);
        } else {
            this.flowBytesPerSec = -1;
            this.flowPacketsPerSec = -1;
        }
        
        // Flow IAT
        this.flowIatMean = Double.isNaN(flow.getFlowIAT().getMean()) ? 0 : flow.getFlowIAT().getMean();
        this.flowIatStd = Double.isNaN(flow.getFlowIAT().getStandardDeviation()) ? 0 : flow.getFlowIAT().getStandardDeviation();
        this.flowIatMax = Double.isNaN(flow.getFlowIAT().getMax()) ? 0 : flow.getFlowIAT().getMax();
        this.flowIatMin = Double.isNaN(flow.getFlowIAT().getMin()) ? 0 : flow.getFlowIAT().getMin();
        
        // Forward IAT
        if (flow.getForward().size() > 1) {
            this.fwdIatTotal = flow.getForwardIAT().getSum();
            this.fwdIatMean = flow.getForwardIAT().getMean();
            this.fwdIatStd = flow.getForwardIAT().getStandardDeviation();
            this.fwdIatMax = flow.getForwardIAT().getMax();
            this.fwdIatMin = flow.getForwardIAT().getMin();
        }
        
        // Backward IAT
        if (flow.getBackward().size() > 1) {
            this.bwdIatTotal = flow.getBackwardIAT().getSum();
            this.bwdIatMean = flow.getBackwardIAT().getMean();
            this.bwdIatStd = flow.getBackwardIAT().getStandardDeviation();
            this.bwdIatMax = flow.getBackwardIAT().getMax();
            this.bwdIatMin = flow.getBackwardIAT().getMin();
        }
        
        // Flag counts
        this.fwdPshFlags = flow.getFPSH_cnt();
        this.bwdPshFlags = flow.getBPSH_cnt();
        this.fwdUrgFlags = flow.getFURG_cnt();
        this.bwdUrgFlags = flow.getBURG_cnt();
        this.fwdRstFlags = flow.getFRST_cnt();
        this.bwdRstFlags = flow.getBRST_cnt();

        // Header lengths
        this.fwdHeaderLength = flow.getFHeaderBytes();
        this.bwdHeaderLength = flow.getBHeaderBytes();
        
        // Packet rates
        this.fwdPacketsPerSec = flow.getfPktsPerSecond();
        this.bwdPacketsPerSec = flow.getbPktsPerSecond();
        
        // Flow length statistics
        if (flow.getForward().size() > 0 || flow.getBackward().size() > 0) {
            this.packetLengthMin = flow.getFlowLengthStats().getMin();
            this.packetLengthMax = flow.getFlowLengthStats().getMax();
            this.packetLengthMean = flow.getFlowLengthStats().getMean();
            this.packetLengthStd = flow.getFlowLengthStats().getStandardDeviation();
            this.packetLengthVar = flow.getFlowLengthStats().getVariance();
        }

        // TCP Flag counts
        this.finCount = flow.getFlagCounts().get("FIN").getValue();
        this.synCount = flow.getFlagCounts().get("SYN").getValue();
        this.rstCount = flow.getFlagCounts().get("RST").getValue();
        this.pshCount = flow.getFlagCounts().get("PSH").getValue();
        this.ackCount = flow.getFlagCounts().get("ACK").getValue();
        this.urgCount = flow.getFlagCounts().get("URG").getValue();
        this.cwrCount = flow.getFlagCounts().get("CWR").getValue();
        this.eceCount = flow.getFlagCounts().get("ECE").getValue();
        
        // Additional metrics
        this.downUpRatio = flow.getDownUpRatio();
        this.avgPacketSize = flow.getAvgPacketSize();
        this.fwdSegmentSizeAvg = flow.fAvgSegmentSize();
        this.bwdSegmentSizeAvg = flow.bAvgSegmentSize();
        
        // Bulk statistics
        this.fwdBytesPerBulkAvg = flow.fAvgBytesPerBulk();
        this.fwdPacketsPerBulkAvg = flow.fAvgPacketsPerBulk();
        this.fwdBulkRateAvg = flow.fAvgBulkRate();
        this.bwdBytesPerBulkAvg = flow.bAvgBytesPerBulk();
        this.bwdPacketsPerBulkAvg = flow.bAvgPacketsPerBulk();
        this.bwdBulkRateAvg = flow.bAvgBulkRate();

        // Subflow statistics
        this.subflowFwdPackets = flow.getSflow_fpackets();
        this.subflowFwdBytes = flow.getSflow_fbytes();
        this.subflowBwdPackets = flow.getSflow_bpackets();
        this.subflowBwdBytes = flow.getSflow_bbytes();
        
        // Window statistics
        this.fwdInitWinBytes = flow.getInit_Win_bytes_forward();
        this.bwdInitWinBytes = flow.getInit_Win_bytes_backward();
        this.fwdActDataPackets = flow.getAct_data_pkt_forward();
        this.bwdActDataPackets = flow.getAct_data_pkt_backward();
        this.fwdSegSizeMin = flow.getmin_seg_size_forward();
        this.bwdSegSizeMin = flow.getmin_seg_size_backward();
        
        if (flow.getFlowActive().getN() > 0) {
            // Active statistics
            this.activeMean = flow.getFlowActive().getMean();
            this.activeStd = flow.getFlowActive().getStandardDeviation();
            this.activeMax = flow.getFlowActive().getMax();
            this.activeMin = flow.getFlowActive().getMin();
        }
        
        // Idle statistics
        if (flow.getFlowIdle().getN() > 0) {
            this.idleMean = flow.getFlowIdle().getMean();
            this.idleStd = flow.getFlowIdle().getStandardDeviation();
            this.idleMax = flow.getFlowIdle().getMax();
            this.idleMin = flow.getFlowIdle().getMin();
        }

        this.icmpCode = flow.getIcmpCode();
        this.icmpType = flow.getIcmpType();

        this.fwdTCPRetransCount = flow.getFwdTcpRetransCnt();
        this.bwdTCPRetransCount = flow.getBwdTcpRetransCnt();
        this.totalTCPRetransCount = this.fwdTCPRetransCount + this.bwdTCPRetransCount;

        this.cummConnectionTime = flow.getCumulativeConnectionDuration();
        
        // Label
        this.label = flow.getLabel();
    }
}
