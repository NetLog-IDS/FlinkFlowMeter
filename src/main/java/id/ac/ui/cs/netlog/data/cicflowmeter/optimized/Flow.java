package id.ac.ui.cs.netlog.data.cicflowmeter.optimized;

import java.util.Arrays;
import java.util.UUID;

import id.ac.ui.cs.netlog.data.cicflowmeter.PacketInfo;
import id.ac.ui.cs.netlog.data.cicflowmeter.ProtocolEnum;
import id.ac.ui.cs.netlog.data.cicflowmeter.TCPFlowState;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Flow {
	private Long firstArrivalTime;
	private Long processStartTime;
	private Statistics sniffStartTimeStats;
	private Long timerDeadline;
	private Long clearanceDeadline;
	private Boolean submitted = false;

    private Statistics fwdPktStats = null;
    private Statistics bwdPktStats = null;

	private Long forwardBytes;
	private Long backwardBytes;
	private Long fwdHeaderBytes;
	private Long bwdHeaderBytes;
	
	private boolean bidirectional;

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

	private Long fwdActDataPkt = 0L;
	private Long bwdActDataPkt = 0L;
	private Long fwdMinSegSize = 0L;
	private Long bwdMinSegSize = 0L;
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
	
	private long fwdBulkDuration = 0;
	private long fwdBulkPacketCount = 0;
	private long fwdBulkSizeTotal = 0;
	private long fwdBulkStateCount = 0;
	private long fwdBulkPacketCountHelper = 0;
	private long fwdBulkStartHelper = 0;
	private long fwdBulkSizeHelper = 0;
	private long fwdLastBulkTime = 0;
	private long bwdBulkDuration = 0;
	private long bwdBulkPacketCount = 0;
	private long bwdBulkSizeTotal = 0;
	private long bwdBulkStateCount = 0;
	private long bwdBulkPacketCountHelper = 0;
	private long bwdBulkStartHelper = 0;
	private long bwdBulkSizeHelper = 0;
	private long bwdLastBulkTime = 0;

	private int fwdTcpRetransCnt = 0;
    private int bwdTcpRetransCnt = 0;

    //To keep track of TCP connection teardown, or an RST packet in one direction.
    private TCPFlowState tcpFlowState;

	private long cummulativeConnectionDuration;

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
		this.sniffStartTimeStats = new Statistics();
		this.flowIAT = new Statistics();
		this.forwardIAT = new Statistics();
		this.backwardIAT = new Statistics();
		this.flowActive = new Statistics();
		this.flowIdle = new Statistics();
		this.flowLengthStats = new Statistics();
        this.fwdPktStats = new Statistics();
		this.bwdPktStats =  new Statistics();
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
		
        this.fwdHeaderBytes = 0L;
        this.bwdHeaderBytes = 0L;
        this.tcpFlowState = null;
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

		updateFlowBulk(packet);
		
		this.endActiveTime = packet.getTimeStamp();
		this.flowStartTime = packet.getTimeStamp();
		this.firstArrivalTime = packet.getArrivalTime();
		this.sniffStartTimeStats.add(packet.getSniffTime());
		this.flowLastSeen = packet.getTimeStamp();
		this.startActiveTime = packet.getTimeStamp();
		detectUpdateSubflows(packet);
		this.flowLengthStats.add(packet.getPayloadBytes());
	
		if (Arrays.equals(this.src, packet.getSrc())) {
			this.fwdMinSegSize = packet.getHeaderBytes();
			this.fwdInitWinBytes = packet.getTCPWindow();
			this.fwdPktStats.add(packet.getPayloadBytes());
			this.fwdHeaderBytes = packet.getHeaderBytes();
			this.forwardLastSeen = packet.getTimeStamp();
			this.forwardBytes += packet.getPayloadBytes();
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
			this.bwdHeaderBytes = packet.getHeaderBytes();
			this.backwardLastSeen = packet.getTimeStamp();
			this.backwardBytes += packet.getPayloadBytes();
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
        this.flowId = UUID.randomUUID().toString();
	}
    
    public void addPacket(PacketInfo packet) {
		updateFlowBulk(packet);
		detectUpdateSubflows(packet);
    	long currentTimestamp = packet.getTimeStamp();
    	if (this.bidirectional) {
			this.flowLengthStats.add(packet.getPayloadBytes());

    		if(Arrays.equals(this.src, packet.getSrc())){
				if(packet.getPayloadBytes() >= 1){
					this.fwdActDataPkt++;
				}
				this.fwdPktStats.add(packet.getPayloadBytes());
				this.fwdHeaderBytes += packet.getHeaderBytes();
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
				this.bwdHeaderBytes += packet.getHeaderBytes();
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
			this.fwdHeaderBytes += packet.getHeaderBytes();
    		this.forwardBytes += packet.getPayloadBytes();
    		this.forwardIAT.add(currentTimestamp - this.forwardLastSeen);
    		this.forwardLastSeen = Math.max(this.forwardLastSeen, currentTimestamp);
			this.fwdMinSegSize = Math.min(packet.getHeaderBytes(), this.fwdMinSegSize);
    	}

    	this.flowIAT.add(packet.getTimeStamp() - this.flowLastSeen);
		this.sniffStartTimeStats.add(packet.getSniffTime());
    	this.flowLastSeen = Math.max(this.flowLastSeen, packet.getTimeStamp());
    }

	public Double calculateFwdPktsPerSecond() {
		long duration = this.flowLastSeen - this.flowStartTime;
		if (duration > 0) {
			return (calculateFwdPacketCount() / ((double) duration / 1000000L));
		} else {
			return 0.0;
		}
	}

	public double calculateBwdPktsPerSecond() {
		long duration = this.flowLastSeen - this.flowStartTime;
		if (duration > 0) {
			return (calculateBwdPacketCount() / ((double) duration / 1000000L));
		}
		else {
			return 0;
		}
	}

	public Double calculateDownUpRatio() {
		if (calculateFwdPacketCount() > 0) {
			return ((double) calculateBwdPacketCount()) / calculateFwdPacketCount();
		}
		return 0.0;
	}

	public Double calculateAvgPacketSize() {
		if (calculatePacketCount() > 0) {
			return ((double) this.flowLengthStats.calculateSum()) / calculatePacketCount();
		}
		return 0.0;
	}

	public Double calculateFwdAvgSegmentSize() {
		if (calculateFwdPacketCount() != 0)
			return (this.fwdPktStats.calculateSum() / (double) calculateFwdPacketCount());
		return 0.0;
	}

	public Double calculateBwdAvgSegmentSize() {
		if (calculateBwdPacketCount() != 0)
			return (this.bwdPktStats.calculateSum() / (double) calculateBwdPacketCount());
		return 0.0;
	}

	public Double calculateSubflowFwdBytes() {
		if (this.subflowCount <= 0) return 0.0;
        return (double) this.forwardBytes / this.subflowCount;
	}

    public Double calculateSubflowFwdPackets() {
        if (this.subflowCount <= 0) return 0.0;
		return (double) this.fwdPktStats.calculateCount() / this.subflowCount;
    }

    public Double calculateSubflowBwdBytes() {
        if (this.subflowCount <= 0) return 0.0;
		return (double) this.backwardBytes / this.subflowCount;
    }

    public Double calculateSubflowBwdPackets() {
        if (this.subflowCount <= 0) return 0.0;
		return (double) this.bwdPktStats.calculateCount() / this.subflowCount;
    }

	void detectUpdateSubflows(PacketInfo packet) {
        if (this.subflowLastPacketTime == -1) {
            this.subflowLastPacketTime = packet.getTimeStamp();
        }
        if(((packet.getTimeStamp() - this.subflowLastPacketTime) / (double) 1000000)  > 1.0){
            this.subflowCount++;
            updateActiveIdleTime(packet.getTimeStamp(), this.activityTimeout);
        }
        this.subflowLastPacketTime = packet.getTimeStamp();
	}

	private void updateFlowBulk(PacketInfo packet) {
		if (Arrays.equals(this.src, packet.getSrc())) {
            updateForwardBulk(packet, bwdLastBulkTime);
        } else {
            updateBackwardBulk(packet, fwdLastBulkTime);
        }
	}

	public void updateForwardBulk(PacketInfo packet, long lastBulkTimeInOther){
        long size = packet.getPayloadBytes();
        if (lastBulkTimeInOther > this.fwdBulkStartHelper) this.fwdBulkStartHelper = 0;
        if (size <= 0) return;

        packet.incrementPayloadPacket();

        if (this.fwdBulkStartHelper == 0) {
            this.fwdBulkStartHelper = packet.getTimeStamp();
            this.fwdBulkPacketCountHelper = 1;
            this.fwdBulkSizeHelper = size;
            this.fwdLastBulkTime = packet.getTimeStamp();
        } // possible bulk
        else {
            // Too much idle time?
            if (((packet.getTimeStamp() - this.fwdLastBulkTime) / (double) 1000000) > 1.0) {
                this.fwdBulkStartHelper = packet.getTimeStamp();
                this.fwdLastBulkTime = packet.getTimeStamp();
                this.fwdBulkPacketCountHelper = 1;
                this.fwdBulkSizeHelper = size;
            } // Add to bulk
            else {
                this.fwdBulkPacketCountHelper += 1;
                this.fwdBulkSizeHelper += size;
                //New bulk
                if (this.fwdBulkPacketCountHelper == 4) {
                    this.fwdBulkStateCount += 1;
                    this.fwdBulkPacketCount += this.fwdBulkPacketCountHelper;
                    this.fwdBulkSizeTotal += this.fwdBulkSizeHelper;
                    this.fwdBulkDuration += packet.getTimeStamp() - this.fwdBulkStartHelper;
                } //Continuation of existing bulk
                else if (this.fwdBulkPacketCountHelper > 4) {
                    this.fwdBulkPacketCount += 1;
                    this.fwdBulkSizeTotal += size;
                    this.fwdBulkDuration += packet.getTimeStamp() - this.fwdLastBulkTime;
                }
                this.fwdLastBulkTime = packet.getTimeStamp();
            }
        }
	}

	public void updateBackwardBulk(PacketInfo packet , long lastBulkTimeInOther){
        long size = packet.getPayloadBytes();
        if (lastBulkTimeInOther > this.bwdBulkStartHelper) this.bwdBulkStartHelper = 0;
        if (size <= 0) return;

        packet.incrementPayloadPacket();

        if (this.bwdBulkStartHelper == 0) {
            this.bwdBulkStartHelper = packet.getTimeStamp();
            this.bwdBulkPacketCountHelper = 1;
            this.bwdBulkSizeHelper = size;
            this.bwdLastBulkTime = packet.getTimeStamp();
        } //possible bulk
        else {
            // Too much idle time?
            if (((packet.getTimeStamp() - this.bwdLastBulkTime) / (double) 1000000) > 1.0) {
                this.bwdBulkStartHelper = packet.getTimeStamp();
                this.bwdLastBulkTime = packet.getTimeStamp();
                this.bwdBulkPacketCountHelper = 1;
                this.bwdBulkSizeHelper = size;
            }// Add to bulk
            else {
                this.bwdBulkPacketCountHelper += 1;
                this.bwdBulkSizeHelper += size;
                //New bulk
                if (this.bwdBulkPacketCountHelper == 4) {
                    this.bwdBulkStateCount += 1;
                    this.bwdBulkPacketCount += this.bwdBulkPacketCountHelper;
                    this.bwdBulkSizeTotal += this.bwdBulkSizeHelper;
                    this.bwdBulkDuration += packet.getTimeStamp() - this.bwdBulkStartHelper;
                } //Continuation of existing bulk
                else if (this.bwdBulkPacketCountHelper > 4) {
                    this.bwdBulkPacketCount += 1;
                    this.bwdBulkSizeTotal += size;
                    this.bwdBulkDuration += packet.getTimeStamp() - this.bwdLastBulkTime;
                }
                this.bwdLastBulkTime = packet.getTimeStamp();
            }
        }
	}

	public Double calculateFwdBulkDurationInSecond() {
		return this.fwdBulkDuration / (double) 1000000;
	}

    // Client average bytes per bulk
    public Double calculateFwdAvgBytesPerBulk() {
        if (this.fwdBulkStateCount != 0) {
			return ((double) this.fwdBulkSizeTotal / this.fwdBulkStateCount);
		}
        return 0.0;
    }

    // Client average packets per bulk
    public Double calculateFwdAvgPacketsPerBulk() {
        if (this.fwdBulkStateCount != 0) {
			return ((double) this.fwdBulkPacketCount / this.fwdBulkStateCount);
		}
        return 0.0;
    }

    // Client average bulk rate
    public Double calculateFwdAvgBulkRate() {
        if (this.fwdBulkDuration != 0) {
			return ((double) this.fwdBulkSizeTotal / calculateFwdBulkDurationInSecond());
		}
        return 0.0;
    }

	public Double calculateBwdBulkDurationInSecond() {
		return this.bwdBulkDuration / (double) 1000000;
	}

    //Server average bytes per bulk
    public Double calculateBwdAvgBytesPerBulk() {
        if (this.bwdBulkStateCount != 0) {
			return ((double) this.bwdBulkSizeTotal / this.bwdBulkStateCount);
		}   
        return 0.0;
    }

    //Server average packets per bulk
    public Double calculateBwdAvgPacketsPerBulk() {
        if (this.bwdBulkStateCount != 0) {
			return ((double) this.bwdBulkPacketCount / this.bwdBulkStateCount);
		}   
        return 0.0;
    }

    //Server average bulk rate
    public Double calculateBwdAvgBulkRate() {
        if (this.bwdBulkDuration != 0) {
			return ((double) this.bwdBulkSizeTotal / calculateBwdBulkDurationInSecond());
		}
        return 0.0;
    }

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
    
    public Integer calculatePacketCount(){
    	if (this.bidirectional) {
			return calculateFwdPacketCount() + calculateBwdPacketCount();
    	} else {
			return calculateFwdPacketCount();
    	}
    }

	public Integer calculateFwdPacketCount() {
		return this.fwdPktStats.calculateCount().intValue();
	}

	public Integer calculateBwdPacketCount() {
		return this.bwdPktStats.calculateCount().intValue();
	}
}

