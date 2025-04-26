package id.ac.ui.cs.netlog.data.cicflowmeter.optimized;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.data.cicflowmeter.ProtocolEnum;
import id.ac.ui.cs.netlog.data.cicflowmeter.TCPFlowState;
import id.ac.ui.cs.netlog.data.cicflowmeter.TCPRetransmission;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Flow {
	private Long processStartTime;
    private Long sniffStartTime;

    private Statistics fwdPktStats = null;
    private Statistics bwdPktStats = null;

	private Long forwardBytes;
	private Long backwardBytes;
	private Long fHeaderBytes;
	private Long bHeaderBytes;
	
	private boolean bidirectional;

// 	private HashMap<String, MutableInt> flagCounts;

	// Flags
	private Integer fwdFINCount;
    private Integer bwdFINCount;

	private Integer fwdSYNCount;
	private Integer bwdSYNCount;

	private Integer fwdRSTCount;
    private Integer bwdRSTCount;

    private Integer fwdPSHCount;
    private Integer bwdPSHCount;

	private Integer fwdACKCount;
    private Integer bwdACKCount;

    private Integer fwdURGCount;
    private Integer bwdURGCount;

	private Integer fwdCWRCount;
    private Integer bwdCWRCount;

	private Integer fwdECECount;
    private Integer bwdECECount;

	private Long fwdActDataPkt;
	private Long bwdActDataPkt;
	private Long fwdMinSegSize;
	private Long bwdMinSegSize;
	private Integer fwdInitWinBytes = 0;
	private Integer bwdInitWinBytes = 0;

	private	byte[] src;
    private byte[] dst;
    private int srcPort;
    private int dstPort;
    private ProtocolEnum protocol;
    private long flowStartTime;
    private long startActiveTime;
    private long endActiveTime;
    private String flowId = null;
    
    private Statistics flowIAT = null;
    private Statistics forwardIAT = null;
    private Statistics backwardIAT = null;
	private Statistics flowLengthStats = null;
    private Statistics flowActive = null;
    private Statistics flowIdle = null;
    
    private	long flowLastSeen;
    private long forwardLastSeen;
    private long backwardLastSeen;
    private long activityTimeout;

	private long subflowLastPacketTime = -1;
    private int subflowCount = 0;
    // private long sfAcHelper = -1;
	
// 	private long fbulkDuration = 0;
// 	private long fbulkPacketCount = 0;
// 	private long fbulkSizeTotal = 0;
// 	private long fbulkStateCount = 0;
// 	private long fbulkPacketCountHelper = 0;
// 	private long fbulkStartHelper = 0;
// 	private long fbulkSizeHelper = 0;
// 	private long flastBulkTS = 0;
// 	private long bbulkDuration = 0;
// 	private long bbulkPacketCount = 0;
// 	private long bbulkSizeTotal = 0;
// 	private long bbulkStateCount = 0;
// 	private long bbulkPacketCountHelper = 0;
// 	private long bbulkStartHelper = 0;
// 	private long bbulkSizeHelper = 0;
// 	private long blastBulkTS = 0;

// 	private int fwdTcpRetransCnt = 0;
//     private int bwdTcpRetransCnt = 0;
//     private Set<TCPRetransmission> tcpPacketsSeen;

//     // The flow timeout is dependent on the user configuration and is unable to capture proper
//     // context in extended TCP connections. This field will help identify whether a flow is
//     // part of an extended TCP connection.
//     private long cumulativeConnectionDuration;

    //To keep track of TCP connection teardown, or an RST packet in one direction.
    private TCPFlowState tcpFlowState;

//     // ICMP fields
//     private int icmpCode = -1;
//     private int icmpType = -1;

	public Flow(
			long processStartTime,
			boolean bidirectional,
			PacketInfo packet,
			byte[] flowSrc,
			byte[] flowDst,
			int flowSrcPort,
			int flowDstPort,
			long activityTimeout) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.bidirectional = bidirectional;
		this.src = flowSrc;
		this.dst = flowDst;
		this.srcPort = flowSrcPort;
		this.dstPort = flowDstPort;
		this.firstPacket(packet);
	}

	public Flow(
			long processStartTime,
			boolean bidirectional,
			PacketInfo packet,
			byte[] flowSrc,
			byte[] flowDst,
			int flowSrcPort,
			int flowDstPort,
			long activityTimeout,
			Set<TCPRetransmission> tcpPacketsSeen) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.bidirectional = bidirectional;
		this.src = flowSrc;
		this.dst = flowDst;
		this.srcPort = flowSrcPort;
		this.dstPort = flowDstPort;
		// this.tcpPacketsSeen = tcpPacketsSeen; // TODO: handle outside
		this.firstPacket(packet);
	}

	public Flow(long processStartTime, boolean bidirectional, PacketInfo packet, long activityTimeout) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.bidirectional = bidirectional;
		this.firstPacket(packet);
	}

	public Flow(long processStartTime, PacketInfo packet, long activityTimeout) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.bidirectional = true;		
		firstPacket(packet);
	}

	public Boolean hasSameDirection(PacketInfo packet) {
		return Arrays.equals(this.src, packet.getSrc());
	}
	
	public void initParameters() {
		this.flowIAT = new Statistics();
		this.forwardIAT = new Statistics();
		this.backwardIAT = new Statistics();
		this.flowActive = new Statistics();
		this.flowIdle = new Statistics();
		this.flowLengthStats = new Statistics();
        this.fwdPktStats = new Statistics();
		this.bwdPktStats =  new Statistics();
		// this.flagCounts = new HashMap<String, MutableInt>();
		// initFlags();
		this.forwardBytes = 0L;
		this.backwardBytes = 0L;	
		this.startActiveTime = 0L;
		this.endActiveTime = 0L;
		this.src = null;
		this.dst = null;

		this.fwdFINCount = 0;
        this.bwdFINCount = 0;
		this.fwdSYNCount = 0;
        this.bwdSYNCount = 0;
		this.fwdRSTCount = 0;
		this.bwdRSTCount = 0;
        this.fwdPSHCount = 0;
        this.bwdPSHCount = 0;
		this.fwdACKCount = 0;
        this.bwdACKCount = 0;
        this.fwdURGCount = 0;
        this.bwdURGCount = 0;
		this.fwdCWRCount = 0;
        this.bwdCWRCount = 0;
		this.fwdECECount = 0;
        this.bwdECECount = 0;
		
        this.fHeaderBytes = 0L;
        this.bHeaderBytes = 0L;
        // this.cumulativeConnectionDuration = 0L;
        this.tcpFlowState = null;
        // this.tcpPacketsSeen = new HashSet<TCPRetransmission>();
	}
	
	
	public void firstPacket(PacketInfo packet) {
		if (this.src == null) {
            this.src = packet.getSrc();
            this.srcPort = packet.getSrcPort();
        }
        if (this.dst == null) {
            this.dst = packet.getDst();
            this.dstPort = packet.getDstPort();
        }

		// updateFlowBulk(packet);
		
		// checkFlags(packet);

		this.endActiveTime = packet.getTimeStamp();
		this.flowStartTime = packet.getTimeStamp();
        this.sniffStartTime = packet.getSniffTime();
		this.flowLastSeen = packet.getTimeStamp();
		this.startActiveTime = packet.getTimeStamp();
		detectUpdateSubflows(packet);
		this.flowLengthStats.add(packet.getPayloadBytes());
	
		if (Arrays.equals(this.src, packet.getSrc())) {
			this.fwdMinSegSize = packet.getHeaderBytes();
			this.fwdInitWinBytes = packet.getTCPWindow();
			this.fwdPktStats.add(packet.getPayloadBytes());
			this.fHeaderBytes = packet.getHeaderBytes();
			this.forwardLastSeen = packet.getTimeStamp();
			this.forwardBytes += packet.getPayloadBytes();
			// this.forward.add(packet);
            if (packet.getPayloadBytes() >= 1) {
				this.fwdActDataPkt++;
            }
			if (packet.isFlagFIN()) {
                this.fwdFINCount++;
            }
			if (packet.isFlagSYN()) {
				this.fwdSYNCount++;
			}
			if (packet.isFlagRST()) {
                this.fwdRSTCount++;
            }
            if (packet.isFlagPSH()) {
                this.fwdPSHCount++;
            }
			if (packet.isFlagACK()) {
                this.fwdACKCount++;
            }
            if (packet.isFlagURG()) {
                this.fwdURGCount++;
            }
			if (packet.isFlagCWR()) {
                this.fwdCWRCount++;
            }
			if (packet.isFlagECE()) {
                this.fwdECECount++;
            }
		} else {
			this.bwdMinSegSize = packet.getHeaderBytes();
			this.bwdInitWinBytes = packet.getTCPWindow();
			this.bwdPktStats.add(packet.getPayloadBytes());
			this.bHeaderBytes = packet.getHeaderBytes();
			this.backwardLastSeen = packet.getTimeStamp();
			this.backwardBytes += packet.getPayloadBytes();
			// this.backward.add(packet);
            if (packet.getPayloadBytes() >= 1) {
				this.bwdActDataPkt++;
            }
			if (packet.isFlagFIN()) {
                this.bwdFINCount++;
            }
			if (packet.isFlagSYN()) {
                this.bwdSYNCount++;
            }
			if (packet.isFlagRST()) {
                this.bwdRSTCount++;
            }
            if (packet.isFlagPSH()) {
                this.bwdPSHCount++;
            }
			if (packet.isFlagACK()) {
                this.bwdACKCount++;
            }
            if (packet.isFlagURG()) {
                this.bwdURGCount++;
            }
			if (packet.isFlagCWR()) {
                this.bwdCWRCount++;
            }
			if (packet.isFlagECE()) {
                this.bwdECECount++;
            }
		}
		this.protocol = packet.getProtocol();
        // this.icmpCode = packet.getIcmpCode();
        // this.icmpType = packet.getIcmpType();
        this.flowId = UUID.randomUUID().toString();
        // handleTcpRetransmissionFields(packet);	
	}

// 	/***
//      * The retransmission mechanism is crude, and relies on the fact that the fields in the TcpRetransmissionDTO
//      * class are unique. This is not a perfect solution, but it should be good enough for detection of very obvious
//      * TCP retransmissions.
//      * @param packet
//      */
//     private void handleTcpRetransmissionFields(PacketInfo packet) {
//         if (this.protocol == ProtocolEnum.TCP) {
//             TCPRetransmission tcpRetransmission = packet.getTcpRetransmission();
//             // If the element was successfully added to the hashset, then it has not been seen
//             // before, and is not a retransmission.
//             boolean isRetransmission = !(this.tcpPacketsSeen.add(tcpRetransmission));
//             if (isRetransmission) {
//                 // check if the packet is a forward packet
//                 if (Arrays.equals(this.src, packet.getSrc())) {
//                     // increment the forward retransmission count
//                     this.fwdTcpRetransCnt++;
//                 } else {
//                     // increment the backward retransmission count
//                     this.bwdTcpRetransCnt++;
//                 }
//             }
//         }
//     }
    
    public void addPacket(PacketInfo packet) {
		// updateFlowBulk(packet);
		detectUpdateSubflows(packet);
		// checkFlags(packet);
		// handleTcpRetransmissionFields(packet);
    	long currentTimestamp = packet.getTimeStamp();
    	if (this.bidirectional) {
			this.flowLengthStats.add(packet.getPayloadBytes());

    		if(Arrays.equals(this.src, packet.getSrc())){
				if(packet.getPayloadBytes() >= 1){
					this.fwdActDataPkt++;
				}
				this.fwdPktStats.add(packet.getPayloadBytes());
				this.fHeaderBytes += packet.getHeaderBytes();
    			// this.forward.add(packet);   
    			this.forwardBytes += packet.getPayloadBytes();
    			if (this.fwdPktStats.calculateCount() > 1)
    				this.forwardIAT.add(currentTimestamp - this.forwardLastSeen);
    			this.forwardLastSeen = Math.max(this.forwardLastSeen, currentTimestamp);
				this.fwdMinSegSize = Math.min(packet.getHeaderBytes(), this.fwdMinSegSize);
				if (packet.isFlagFIN()) {
					this.fwdFINCount++;
				}
				if (packet.isFlagSYN()) {
					this.fwdSYNCount++;
				}
				if (packet.isFlagRST()) {
					this.fwdRSTCount++;
				}
				if (packet.isFlagPSH()) {
					this.fwdPSHCount++;
				}
				if (packet.isFlagACK()) {
					this.fwdACKCount++;
				}
				if (packet.isFlagURG()) {
					this.fwdURGCount++;
				}
				if (packet.isFlagCWR()) {
					this.fwdCWRCount++;
				}
				if (packet.isFlagECE()) {
					this.fwdECECount++;
				}
    		}else{
				if (packet.getPayloadBytes() >= 1) {
					this.bwdActDataPkt++;
                }
				this.bwdPktStats.add(packet.getPayloadBytes());
				// set Init_win_bytes_backward (now bwdInitWinBytes) if not been set. The set logic isn't 100%
                // accurate, since it technically takes the first non-zero value, but should
                // be good enough for most cases.
                if (this.bwdInitWinBytes == 0) {
                    this.bwdInitWinBytes = packet.getTCPWindow();
                }
				this.bHeaderBytes += packet.getHeaderBytes();
    			// this.backward.add(packet);
    			this.backwardBytes += packet.getPayloadBytes();
    			if (this.bwdPktStats.calculateCount() > 1)
    				this.backwardIAT.add(currentTimestamp - this.backwardLastSeen);
    			this.backwardLastSeen = Math.max(backwardLastSeen, currentTimestamp);
				this.bwdMinSegSize = Math.min(packet.getHeaderBytes(), this.bwdMinSegSize);
				if (packet.isFlagFIN()) {
					this.bwdFINCount++;
				}
				if (packet.isFlagSYN()) {
					this.bwdSYNCount++;
				}
				if (packet.isFlagRST()) {
					this.bwdRSTCount++;
				}
				if (packet.isFlagPSH()) {
					this.bwdPSHCount++;
				}
				if (packet.isFlagACK()) {
					this.bwdACKCount++;
				}
				if (packet.isFlagURG()) {
					this.bwdURGCount++;
				}
				if (packet.isFlagCWR()) {
					this.bwdCWRCount++;
				}
				if (packet.isFlagECE()) {
					this.bwdECECount++;
				}
    		}
    	} else {
			if(packet.getPayloadBytes() >= 1) {
				this.fwdActDataPkt++;
			}
			this.fwdPktStats.add(packet.getPayloadBytes());
			this.flowLengthStats.add(packet.getPayloadBytes());
			this.fHeaderBytes += packet.getHeaderBytes();
    		// this.forward.add(packet);    		
    		this.forwardBytes += packet.getPayloadBytes();
    		this.forwardIAT.add(currentTimestamp - this.forwardLastSeen);
    		this.forwardLastSeen = Math.max(this.forwardLastSeen, currentTimestamp);
			this.fwdMinSegSize = Math.min(packet.getHeaderBytes(), this.fwdMinSegSize);
    	}

    	this.flowIAT.add(packet.getTimeStamp() - this.flowLastSeen);
    	this.flowLastSeen = Math.max(this.flowLastSeen, packet.getTimeStamp());
    }

// 	public double getfPktsPerSecond() {
// 		long duration = this.flowLastSeen - this.flowStartTime;
// 		if (duration > 0) {
// 			return (this.forward.size() / ((double) duration / 1000000L));
// 		}
// 		else
// 			return 0;
// 	}

// 	public double getbPktsPerSecond() {
// 		long duration = this.flowLastSeen - this.flowStartTime;
// 		if (duration > 0) {
// 			return (this.backward.size() / ((double) duration / 1000000L));
// 		}
// 		else
// 			return 0;
// 	}

// 	public double getDownUpRatio() {
// 		if (this.forward.size() > 0) {
// 			return ((double)this.backward.size())/this.forward.size();
// 		}
// 		return 0;
// 	}

// 	public double getAvgPacketSize() {
// 		if (this.packetCount() > 0) {
// 			return (this.flowLengthStats.getSum() / this.packetCount());
// 		}
// 		return 0;
// 	}

// 	public double fAvgSegmentSize() {
// 		if (this.forward.size() != 0)
// 			return (this.fwdPktStats.getSum() / (double) this.forward.size());
// 		return 0;
// 	}

// 	public double bAvgSegmentSize() {
// 		if (this.backward.size() != 0)
// 			return (this.bwdPktStats.getSum() / (double) this.backward.size());
// 		return 0;
// 	}

//     public void initFlags() {
// 		flagCounts.put("FIN", new MutableInt());
// 		flagCounts.put("SYN", new MutableInt());
// 		flagCounts.put("RST", new MutableInt());
// 		flagCounts.put("PSH", new MutableInt());
// 		flagCounts.put("ACK", new MutableInt());
// 		flagCounts.put("URG", new MutableInt());
// 		flagCounts.put("CWR", new MutableInt());
// 		flagCounts.put("ECE", new MutableInt());
// 	}

// 	public void checkFlags(PacketInfo packet){
// 		if (packet.isFlagFIN()) {
// 			flagCounts.get("FIN").increment();
// 		}
// 		if (packet.isFlagSYN()) {
// 			flagCounts.get("SYN").increment();
// 		}
// 		if (packet.isFlagRST()) {
// 			flagCounts.get("RST").increment();
// 		}
// 		if (packet.isFlagPSH()) {
// 			flagCounts.get("PSH").increment();
// 		}
// 		if (packet.isFlagACK()) {
// 			flagCounts.get("ACK").increment();
// 		}
// 		if (packet.isFlagURG()) {
// 			flagCounts.get("URG").increment();
// 		}
// 		if (packet.isFlagCWR()) {
// 			flagCounts.get("CWR").increment();
// 		}
// 		if (packet.isFlagECE()) {
// 			flagCounts.get("ECE").increment();
// 		}
// 	}

	public Double calculateSubflowFwdBytes() {
		if (this.subflowCount <= 0) return 0.0;
        return (double) this.forwardBytes / this.subflowCount;
	}

    public Double calculateSubflowFwdPackets() {
        if (this.subflowCount <= 0) return 0.0;
		return (double) this.fwdPktStats.calculateCount() / this.subflowCount;
        // return (double) this.forward.size() / sfCount;
    }

    public Double calculateSubflowBwdBytes() {
        if (this.subflowCount <= 0) return 0.0;
		return (double) this.backwardBytes / this.subflowCount;
        // return (double) this.backwardBytes / sfCount;
    }

    public Double calculateSubflowBwdPackets() {
        if (this.subflowCount <= 0) return 0.0;
		return (double) this.bwdPktStats.calculateCount() / this.subflowCount;
        // return (double) this.backward.size() / sfCount;
    }

	void detectUpdateSubflows(PacketInfo packet) {
        if (this.subflowLastPacketTime == -1) {
            this.subflowLastPacketTime = packet.getTimeStamp();
            // sfAcHelper = packet.getTimeStamp();
        }
        if(((packet.getTimeStamp() - this.subflowLastPacketTime) / (double) 1000000)  > 1.0){
            this.subflowCount++;
            updateActiveIdleTime(packet.getTimeStamp(), this.activityTimeout);
            // sfAcHelper = packet.getTimeStamp();
        }
        this.subflowLastPacketTime = packet.getTimeStamp();
	}

// 	public void updateFlowBulk(PacketInfo packet) {
// 		if (Arrays.equals(this.src, packet.getSrc())) {
//             updateForwardBulk(packet, blastBulkTS);
//         } else {
//             updateBackwardBulk(packet,flastBulkTS);
//         }
// 	}

// 	public void updateForwardBulk(PacketInfo packet, long tsOflastBulkInOther){
//         long size = packet.getPayloadBytes();
//         if (tsOflastBulkInOther > fbulkStartHelper) fbulkStartHelper = 0;
//         if (size <= 0) return;

//         packet.getPayloadPacket();

//         if (fbulkStartHelper == 0) {
//             fbulkStartHelper = packet.getTimeStamp();
//             fbulkPacketCountHelper = 1;
//             fbulkSizeHelper = size;
//             flastBulkTS = packet.getTimeStamp();
//         } //possible bulk
//         else {
//             // Too much idle time?
//             if (((packet.getTimeStamp() - flastBulkTS) / (double) 1000000) > 1.0) {
//                 fbulkStartHelper = packet.getTimeStamp();
//                 flastBulkTS = packet.getTimeStamp();
//                 fbulkPacketCountHelper = 1;
//                 fbulkSizeHelper = size;
//             }// Add to bulk
//             else {
//                 fbulkPacketCountHelper += 1;
//                 fbulkSizeHelper += size;
//                 //New bulk
//                 if (fbulkPacketCountHelper == 4) {
//                     fbulkStateCount += 1;
//                     fbulkPacketCount += fbulkPacketCountHelper;
//                     fbulkSizeTotal += fbulkSizeHelper;
//                     fbulkDuration += packet.getTimeStamp() - fbulkStartHelper;
//                 } //Continuation of existing bulk
//                 else if (fbulkPacketCountHelper > 4) {
//                     fbulkPacketCount += 1;
//                     fbulkSizeTotal += size;
//                     fbulkDuration += packet.getTimeStamp() - flastBulkTS;
//                 }
//                 flastBulkTS = packet.getTimeStamp();
//             }
//         }
// 	}

// 	public void updateBackwardBulk(PacketInfo packet , long tsOflastBulkInOther){
// 		/*bAvgBytesPerBulk =0;
// 		bbulkSizeTotal=0;
// 		bbulkStateCount=0;*/
//         long size = packet.getPayloadBytes();
//         if (tsOflastBulkInOther > bbulkStartHelper) bbulkStartHelper = 0;
//         if (size <= 0) return;

//         packet.getPayloadPacket();

//         if (bbulkStartHelper == 0) {
//             bbulkStartHelper = packet.getTimeStamp();
//             bbulkPacketCountHelper = 1;
//             bbulkSizeHelper = size;
//             blastBulkTS = packet.getTimeStamp();
//         } //possible bulk
//         else {
//             // Too much idle time?
//             if (((packet.getTimeStamp() - blastBulkTS) / (double) 1000000) > 1.0) {
//                 bbulkStartHelper = packet.getTimeStamp();
//                 blastBulkTS = packet.getTimeStamp();
//                 bbulkPacketCountHelper = 1;
//                 bbulkSizeHelper = size;
//             }// Add to bulk
//             else {
//                 bbulkPacketCountHelper += 1;
//                 bbulkSizeHelper += size;
//                 //New bulk
//                 if (bbulkPacketCountHelper == 4) {
//                     bbulkStateCount += 1;
//                     bbulkPacketCount += bbulkPacketCountHelper;
//                     bbulkSizeTotal += bbulkSizeHelper;
//                     bbulkDuration += packet.getTimeStamp() - bbulkStartHelper;
//                 } //Continuation of existing bulk
//                 else if (bbulkPacketCountHelper > 4) {
//                     bbulkPacketCount += 1;
//                     bbulkSizeTotal += size;
//                     bbulkDuration += packet.getTimeStamp() - blastBulkTS;
//                 }
//                 blastBulkTS = packet.getTimeStamp();
//             }
//         }
// 	}

// 	public long fbulkStateCount() {
// 		return fbulkStateCount;
// 	}

// 	public long fbulkSizeTotal() {
// 		return fbulkSizeTotal;
// 	}

// 	public long fbulkPacketCount() {
// 		return fbulkPacketCount;
// 	}

// 	public long fbulkDuration() {
// 		return fbulkDuration;
// 	}
// 	public double fbulkDurationInSecond() {
// 		return fbulkDuration / (double) 1000000;
// 	}

//     //Client average bytes per bulk
//     public double fAvgBytesPerBulk() {
//         if (this.fbulkStateCount() != 0)
//             return ((double) this.fbulkSizeTotal() / this.fbulkStateCount());
//         return 0;
//     }

//     //Client average packets per bulk
//     public double fAvgPacketsPerBulk() {
//         if (this.fbulkStateCount() != 0)
//             return ((double) this.fbulkPacketCount() / this.fbulkStateCount());
//         return 0;
//     }

//     //Client average bulk rate
//     public double fAvgBulkRate() {
//         if (this.fbulkDuration() != 0)
//             return ((double) this.fbulkSizeTotal() / this.fbulkDurationInSecond());
//         return 0;
//     }

// 	//new features server
// 	public long bbulkPacketCount() {
// 		return bbulkPacketCount;
// 	}

// 	public long bbulkStateCount() {
// 		return bbulkStateCount;
// 	}

// 	public long bbulkSizeTotal() {
// 		return bbulkSizeTotal;
// 	}

// 	public long bbulkDuration() {
// 		return bbulkDuration;
// 	}

// 	public double bbulkDurationInSecond() {
// 		return bbulkDuration / (double) 1000000;
// 	}

//     //Server average bytes per bulk
//     public double bAvgBytesPerBulk() {
//         if (this.bbulkStateCount() != 0)
//             return ((double) this.bbulkSizeTotal() / this.bbulkStateCount());
//         return 0;
//     }

//     //Server average packets per bulk
//     public double bAvgPacketsPerBulk() {
//         if (this.bbulkStateCount() != 0)
//             return ((double) this.bbulkPacketCount() / this.bbulkStateCount());
//         return 0;
//     }

//     //Server average bulk rate
//     public double bAvgBulkRate() {
//         if (this.bbulkDuration() != 0)
//             return ((double) this.bbulkSizeTotal() / this.bbulkDurationInSecond());
//         return 0;
//     }

    public void updateActiveIdleTime(long currentTime, long threshold) {
        if ((currentTime - this.endActiveTime) > threshold) {
            if ((this.endActiveTime - this.startActiveTime) > 0) {
                this.flowActive.add(this.endActiveTime - this.startActiveTime);
            }
            this.flowIdle.add(currentTime - this.endActiveTime);
            this.startActiveTime = currentTime;
            this.endActiveTime = currentTime;
        } else {
            this.endActiveTime = currentTime;
        }
    }
    
//     public void endActiveIdleTime(long currentTime, long threshold, long flowTimeOut, boolean isFlagEnd) {
//         if ((this.endActiveTime - this.startActiveTime) > 0) {
//             this.flowActive.addValue(this.endActiveTime - this.startActiveTime);
//         }

//         if (!isFlagEnd && ((flowTimeOut - (this.endActiveTime - this.flowStartTime)) > 0)) {
//             this.flowIdle.addValue(flowTimeOut - (this.endActiveTime - this.flowStartTime));
//         }
//     }
    
//     public int packetCount(){
//     	if (isBidirectional) {
//     		return (this.forward.size() + this.backward.size()); 
//     	} else {
//     		return this.forward.size();    		
//     	}
//     }

// 	public boolean isBidirectional() {
//         return isBidirectional;
//     }

//     public void setBidirectional(boolean isBidirectional) {
//         this.isBidirectional = isBidirectional;
//     }
    
// 	public List<PacketInfo> getForward() {
// 		return new ArrayList<>(forward);
// 	}

// 	public List<PacketInfo> getBackward() {
// 		return new ArrayList<>(backward);
// 	}

// 	public byte[] getSrc() {
// 		return Arrays.copyOf(src, src.length);
// 	}

// 	public byte[] getDst() {
// 		return Arrays.copyOf(dst, dst.length);
// 	}
	
// 	public String getProtocolStr() {
// 		switch(this.protocol.val){
// 		case(6):
// 			return "TCP";
// 		case(17):
// 		    return "UDP";
// 		}
// 		return "UNKNOWN";
// 	}

// 	public long getLastSeen() {
// 		return flowLastSeen;
// 	}

// 	public void setLastSeen(long lastSeen) {
// 		this.flowLastSeen = lastSeen;
// 	}

// 	public String getSrcIP() {
// 		return PacketUtils.byteArrayToIp(src);
// 	}
	
// 	public String getDstIP() {
// 		return PacketUtils.byteArrayToIp(dst);
// 	}
	
// 	public String getTimeStamp() {
// 		return DateUtils.parseDateFromLong(flowStartTime / 1000L, "dd/MM/yyyy hh:mm:ss");
// 	}
	
// 	public long getFlowDuration() {
// 		return flowLastSeen - flowStartTime;
// 	}
	
// 	public long getTotalFwdPackets() {
// 		return fwdPktStats.getN();
// 	}
	
// 	public long getTotalBackwardPackets() {
// 		return bwdPktStats.getN();
// 	}
	
// 	public double getTotalLengthofFwdPackets() {
// 		return fwdPktStats.getSum();
// 	}
	
// 	public double getTotalLengthofBwdPackets() {
// 		return bwdPktStats.getSum();
// 	}
	
//     public double getFwdPacketLengthMax() {
//         return (fwdPktStats.getN() > 0L) ? fwdPktStats.getMax() : 0;
//     }

//     public double getFwdPacketLengthMin() {
//         return (fwdPktStats.getN() > 0L) ? fwdPktStats.getMin() : 0;
//     }

//     public double getFwdPacketLengthMean() {
//         return (fwdPktStats.getN() > 0L) ? fwdPktStats.getMean() : 0;
//     }

//     public double getFwdPacketLengthStd() {
//         return (fwdPktStats.getN() > 0L) ? fwdPktStats.getStandardDeviation() : 0;
//     }

//     public double getBwdPacketLengthMax() {
//         return (bwdPktStats.getN() > 0L) ? bwdPktStats.getMax() : 0;
//     }

//     public double getBwdPacketLengthMin() {
//         return (bwdPktStats.getN() > 0L) ? bwdPktStats.getMin() : 0;
//     }

//     public double getBwdPacketLengthMean() {
//         return (bwdPktStats.getN() > 0L) ? bwdPktStats.getMean() : 0;
//     }

//     public double getBwdPacketLengthStd() {
//         return (bwdPktStats.getN() > 0L) ? bwdPktStats.getStandardDeviation() : 0;
//     }

//     public double getFlowBytesPerSec() {
//         //flow duration is in microseconds, therefore packets per seconds = packets / (duration/1000000)
//         return ((double) (forwardBytes + backwardBytes)) / ((double) getFlowDuration() / 1000000L);
//     }

//     public double getFlowPacketsPerSec() {
//         return ((double) packetCount()) / ((double) getFlowDuration() / 1000000L);
//     }

//     public SummaryStatistics getFlowIAT() {
//         return flowIAT;
//     }

//     public double getFwdIATTotal() {
//         return (forward.size() > 1) ? forwardIAT.getSum() : 0;
//     }

//     public double getFwdIATMean() {
//         return (forward.size() > 1) ? forwardIAT.getMean() : 0;
//     }

//     public double getFwdIATStd() {
//         return (forward.size() > 1) ? forwardIAT.getStandardDeviation() : 0;
//     }

//     public double getFwdIATMax() {
//         return (forward.size() > 1) ? forwardIAT.getMax() : 0;
//     }

//     public double getFwdIATMin() {
//         return (forward.size() > 1) ? forwardIAT.getMin() : 0;
//     }

//     public double getBwdIATTotal() {
//         return (backward.size() > 1) ? backwardIAT.getSum() : 0;
//     }

//     public double getBwdIATMean() {
//         return (backward.size() > 1) ? backwardIAT.getMean() : 0;
//     }

//     public double getBwdIATStd() {
//         return (backward.size() > 1) ? backwardIAT.getStandardDeviation() : 0;
//     }

//     public double getBwdIATMax() {
//         return (backward.size() > 1) ? backwardIAT.getMax() : 0;
//     }

//     public double getBwdIATMin() {
//         return (backward.size() > 1) ? backwardIAT.getMin() : 0;
//     }
	
// 	public int getFwdPSHFlags() {
// 		return fPSH_cnt;
// 	}
	
// 	public int getBwdPSHFlags() {
// 		return bPSH_cnt;
// 	}
	
// 	public int getFwdURGFlags() {
// 		return fURG_cnt;
// 	}
	
// 	public int getBwdURGFlags() {
// 		return bURG_cnt;
// 	}
	
// 	public int getFwdFINFlags() {
// 		return fFIN_cnt;
// 	}
	
// 	public int getBwdFINFlags() {
// 		return bFIN_cnt;
// 	}
	
// 	public long getFwdHeaderLength() {
// 		return fHeaderBytes;
// 	}
	
// 	public long getBwdHeaderLength() {
// 		return bHeaderBytes;
// 	}
	
//     public double getMinPacketLength() {
//         return (forward.size() > 0 || backward.size() > 0) ? flowLengthStats.getMin() : 0;
//     }

//     public double getMaxPacketLength() {
//         return (forward.size() > 0 || backward.size() > 0) ? flowLengthStats.getMax() : 0;
//     }

//     public double getPacketLengthMean() {
//         return (forward.size() > 0 || backward.size() > 0) ? flowLengthStats.getMean() : 0;
//     }

//     public double getPacketLengthStd() {
//         return (forward.size() > 0 || backward.size() > 0) ? flowLengthStats.getStandardDeviation() : 0;
//     }

//     public double getPacketLengthVariance() {
//         return (forward.size() > 0 || backward.size() > 0) ? flowLengthStats.getVariance() : 0;
//     }

//     public int getFlagCount(String key) {
//         return flagCounts.get(key).getValue();
//     }

//     public double getActiveMean() {
//         return (flowActive.getN() > 0) ? flowActive.getMean() : 0;
//     }

//     public double getActiveStd() {
//         return (flowActive.getN() > 0) ? flowActive.getStandardDeviation() : 0;
//     }

//     public double getActiveMax() {
//         return (flowActive.getN() > 0) ? flowActive.getMax() : 0;
//     }

//     public double getActiveMin() {
//         return (flowActive.getN() > 0) ? flowActive.getMin() : 0;
//     }

//     public double getIdleMean() {
//         return (flowIdle.getN() > 0) ? flowIdle.getMean() : 0;
//     }

//     public double getIdleStd() {
//         return (flowIdle.getN() > 0) ? flowIdle.getStandardDeviation() : 0;
//     }

//     public double getIdleMax() {
//         return (flowIdle.getN() > 0) ? flowIdle.getMax() : 0;
//     }

//     public double getIdleMin() {
//         return (flowIdle.getN() > 0) ? flowIdle.getMin() : 0;
//     }
	
// 	public String getLabel() {
//         return "NeedManualLabel";
//     }
}

