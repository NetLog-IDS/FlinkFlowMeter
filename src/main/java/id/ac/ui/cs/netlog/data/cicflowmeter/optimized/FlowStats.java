package id.ac.ui.cs.netlog.data.cicflowmeter.optimized;

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
    private String sniffStartTime; // Sniff Start Time

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
    
    public FlowStats(Flow flow) {
        // Basic flow identifiers
        this.fid = flow.getFlowId();
        this.srcIp = PacketUtils.byteArrayToIp(flow.getSrc());
        this.srcPort = flow.getSrcPort();
        this.dstIp = PacketUtils.byteArrayToIp(flow.getDst());
        this.dstPort = flow.getDstPort();
        this.protocol = flow.getProtocol().val;
        
        // Timestamp
        this.timestamp = DateUtils.convertEpochTimestamp2String(flow.getFlowStartTime());
        this.sniffStartTime = DateUtils.convertEpochTimestamp2String(flow.getSniffStartTime());
        
        // Flow duration
        this.flowDuration = flow.getFlowLastSeen() - flow.getFlowStartTime();

        this.totalFwdPackets = flow.getFwdPktStats().calculateCount();
        this.totalBwdPackets = flow.getBwdPktStats().calculateCount();
        this.totalFwdLength = flow.getFwdPktStats().calculateSum();
        this.totalBwdLength = flow.getBwdPktStats().calculateSum();
        
        // Packet statistics
        // Forward packets
        if (flow.getFwdPktStats().calculateCount() > 0L) {
            this.fwdPacketLengthMax = flow.getFwdPktStats().calculateMax();
            this.fwdPacketLengthMin = flow.getFwdPktStats().calculateMin();
            this.fwdPacketLengthMean = flow.getFwdPktStats().calculateAvg();
            this.fwdPacketLengthStd = flow.getFwdPktStats().calculateStd();
        }
        
        // Backward packets
        if (flow.getBwdPktStats().calculateCount() > 0L) {
            this.bwdPacketLengthMax = flow.getBwdPktStats().calculateMax();
            this.bwdPacketLengthMin = flow.getBwdPktStats().calculateMin();
            this.bwdPacketLengthMean = flow.getBwdPktStats().calculateAvg();
            this.bwdPacketLengthStd = flow.getBwdPktStats().calculateStd();
        }

        // Flow rates
        if (this.flowDuration != 0) {
            this.flowBytesPerSec = ((double) (flow.getForwardBytes() + flow.getBackwardBytes())) / ((double) this.flowDuration / 1000000L);
            this.flowPacketsPerSec = ((double) flow.calculatePacketCount()) / ((double) this.flowDuration / 1000000L);
        } else {
            this.flowBytesPerSec = -1;
            this.flowPacketsPerSec = -1;
        }
        
        // Flow IAT
        // TODO: check for NaN handling
        // TODO: get to calculate
        this.flowIatMean = flow.getFlowIAT().calculateAvg();
        this.flowIatStd = flow.getFlowIAT().calculateStd();
        this.flowIatMax = flow.getFlowIAT().calculateMax().equals(Long.MIN_VALUE) ? 0 : flow.getFlowIAT().calculateMax();
        this.flowIatMin = flow.getFlowIAT().calculateMin().equals(Long.MAX_VALUE) ? 0 : flow.getFlowIAT().calculateMin();
        // this.flowIatMean = Double.isNaN(flow.getFlowIAT().getMean()) ? 0 : flow.getFlowIAT().getMean();
        // this.flowIatStd = Double.isNaN(flow.getFlowIAT().getStandardDeviation()) ? 0 : flow.getFlowIAT().getStandardDeviation();
        // this.flowIatMax = Double.isNaN(flow.getFlowIAT().getMax()) ? 0 : flow.getFlowIAT().getMax();
        // this.flowIatMin = Double.isNaN(flow.getFlowIAT().getMin()) ? 0 : flow.getFlowIAT().getMin();
        
        // Forward IAT
        if (flow.calculateFwdPacketCount() > 1) {
            this.fwdIatTotal = flow.getForwardIAT().calculateSum();
            this.fwdIatMean = flow.getForwardIAT().calculateAvg();
            this.fwdIatStd = flow.getForwardIAT().calculateStd();
            this.fwdIatMax = flow.getForwardIAT().calculateMax();
            this.fwdIatMin = flow.getForwardIAT().calculateMin();
        }
        
        // // Backward IAT
        if (flow.calculateBwdPacketCount() > 1) {
            this.bwdIatTotal = flow.getBackwardIAT().calculateSum();
            this.bwdIatMean = flow.getBackwardIAT().calculateAvg();
            this.bwdIatStd = flow.getBackwardIAT().calculateStd();
            this.bwdIatMax = flow.getBackwardIAT().calculateMax();
            this.bwdIatMin = flow.getBackwardIAT().calculateMin();
        }
        
        // // Flag counts
        this.fwdPshFlags = flow.getFwdPSHCount();
        this.bwdPshFlags = flow.getBwdPSHCount();
        this.fwdUrgFlags = flow.getFwdURGCount();
        this.bwdUrgFlags = flow.getBwdURGCount();
        this.fwdRstFlags = flow.getFwdRSTCount();
        this.bwdRstFlags = flow.getBwdRSTCount();

        // Header lengths
        this.fwdHeaderLength = flow.getFwdHeaderBytes();
        this.bwdHeaderLength = flow.getBwdHeaderBytes();
        
        // Packet rates
        this.fwdPacketsPerSec = flow.calculateFwdPktsPerSecond();
        this.bwdPacketsPerSec = flow.calculateBwdPktsPerSecond();
        
        // Flow length statistics
        if (flow.calculateFwdPacketCount() > 0 || flow.calculateBwdPacketCount() > 0) {
            this.packetLengthMin = flow.getFlowLengthStats().calculateMin();
            this.packetLengthMax = flow.getFlowLengthStats().calculateMax();
            this.packetLengthMean = flow.getFlowLengthStats().calculateAvg();
            this.packetLengthStd = flow.getFlowLengthStats().calculateStd();
            this.packetLengthVar = flow.getFlowLengthStats().calculateVariance();
        }

        // TCP Flag counts
        this.finCount = flow.getFwdFINCount() + flow.getBwdFINCount();
        this.synCount = flow.getFwdSYNCount() + flow.getBwdSYNCount();
        this.rstCount = flow.getFwdRSTCount() + flow.getBwdRSTCount();
        this.pshCount = flow.getFwdPSHCount() + flow.getBwdPSHCount();
        this.ackCount = flow.getFwdACKCount() + flow.getBwdACKCount();
        this.urgCount = flow.getFwdURGCount() + flow.getBwdURGCount();
        this.cwrCount = flow.getFwdCWRCount() + flow.getBwdCWRCount();
        this.eceCount = flow.getFwdECECount() + flow.getBwdECECount();
        
        // Additional metrics
        this.downUpRatio = flow.calculateDownUpRatio();
        this.avgPacketSize = flow.calculateAvgPacketSize();
        this.fwdSegmentSizeAvg = flow.calculateFwdAvgSegmentSize();
        this.bwdSegmentSizeAvg = flow.calculateBwdAvgSegmentSize();
        
        // Bulk statistics
        this.fwdBytesPerBulkAvg = flow.calculateFwdAvgBytesPerBulk();
        this.fwdPacketsPerBulkAvg = flow.calculateFwdAvgPacketsPerBulk();
        this.fwdBulkRateAvg = flow.calculateFwdAvgBulkRate();
        this.bwdBytesPerBulkAvg = flow.calculateBwdAvgBytesPerBulk();
        this.bwdPacketsPerBulkAvg = flow.calculateBwdAvgPacketsPerBulk();
        this.bwdBulkRateAvg = flow.calculateBwdAvgBulkRate();

        // Subflow statistics
        this.subflowFwdPackets = flow.calculateSubflowFwdPackets();
        this.subflowFwdBytes = flow.calculateSubflowFwdBytes();
        this.subflowBwdPackets = flow.calculateSubflowBwdPackets();
        this.subflowBwdBytes = flow.calculateSubflowBwdBytes();
        
        // Window statistics
        this.fwdInitWinBytes = flow.getFwdInitWinBytes();
        this.bwdInitWinBytes = flow.getBwdInitWinBytes();
        this.fwdActDataPackets = flow.getFwdActDataPkt();
        this.bwdActDataPackets = flow.getBwdActDataPkt();
        this.fwdSegSizeMin = flow.getFwdMinSegSize();
        this.bwdSegSizeMin = flow.getBwdMinSegSize();
        
        // Active statistics
        if (flow.getFlowActive().calculateCount() > 0) {
            this.activeMean = flow.getFlowActive().calculateAvg();
            this.activeStd = flow.getFlowActive().calculateStd();
            this.activeMax = flow.getFlowActive().calculateMax();
            this.activeMin = flow.getFlowActive().calculateMin();
        }
        
        // Idle statistics
        if (flow.getFlowIdle().calculateCount() > 0) {
            this.idleMean = flow.getFlowIdle().calculateAvg();
            this.idleStd = flow.getFlowIdle().calculateStd();
            this.idleMax = flow.getFlowIdle().calculateMax();
            this.idleMin = flow.getFlowIdle().calculateMin();
        }

        // this.icmpCode = flow.getIcmpCode();
        // this.icmpType = flow.getIcmpType();

        // this.fwdTCPRetransCount = flow.getFwdTcpRetransCnt();
        // this.bwdTCPRetransCount = flow.getBwdTcpRetransCnt();
        // this.totalTCPRetransCount = this.fwdTCPRetransCount + this.bwdTCPRetransCount;

        // this.cummConnectionTime = flow.getCumulativeConnectionDuration();
    }
}
