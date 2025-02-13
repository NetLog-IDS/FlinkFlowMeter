package id.ac.ui.cs.netlog.data.cicflowmeter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import id.ac.ui.cs.netlog.utils.DateUtils;
import id.ac.ui.cs.netlog.utils.MutableInt;
import id.ac.ui.cs.netlog.utils.PacketUtils;
import lombok.Data;

@Data
public class Flow {
    private final static String separator = ",";

	private long processStartTime;

	private     SummaryStatistics 		fwdPktStats = null;
	private		SummaryStatistics 		bwdPktStats = null;
	private 	List<PacketInfo> 	forward = null;
	private		List<PacketInfo> 	backward = null;

	private 	long forwardBytes;
	private 	long backwardBytes;
	private 	long fHeaderBytes;
	private 	long bHeaderBytes;
	
	private 	boolean isBidirectional;

	private 	HashMap<String, MutableInt> flagCounts;

	private 	int fPSH_cnt;
	private 	int bPSH_cnt;
	private 	int fURG_cnt;
	private 	int bURG_cnt;
	private     int fFIN_cnt;
	private     int bFIN_cnt;

	private 	long Act_data_pkt_forward;
	private 	long min_seg_size_forward;
	private 	int Init_Win_bytes_forward=0;
	private 	int Init_Win_bytes_backward=0;


	private		byte[] src;
    private    	byte[] dst;
    private    	int    srcPort;
    private    	int    dstPort;
    private    	int    protocol;
    private    	long   flowStartTime;
    private    	long   startActiveTime;
    private    	long   endActiveTime;
    private    	String flowId = null;
    
    private     SummaryStatistics flowIAT = null;
    private     SummaryStatistics forwardIAT = null;
    private     SummaryStatistics backwardIAT = null;
	private     SummaryStatistics flowLengthStats = null;
    private     SummaryStatistics flowActive = null;
    private     SummaryStatistics flowIdle = null;
    
    private	    long   flowLastSeen;
    private     long   forwardLastSeen;
    private     long   backwardLastSeen;
    private     long   activityTimeout;

	public Flow(
			long processStartTime,
			boolean isBidirectional,
			PacketInfo packet,
			byte[] flowSrc,
			byte[] flowDst,
			int flowSrcPort,
			int flowDstPort,
			long activityTimeout) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.isBidirectional = isBidirectional;
		this.firstPacket(packet);
		this.src = flowSrc;
		this.dst = flowDst;
		this.srcPort = flowSrcPort;
		this.dstPort = flowDstPort;
	}    
    
	public Flow(long processStartTime, boolean isBidirectional, PacketInfo packet, long activityTimeout) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.isBidirectional = isBidirectional;
		this.firstPacket(packet);
	}

	public Flow(long processStartTime, PacketInfo packet, long activityTimeout) {
		this.processStartTime = processStartTime;
		this.activityTimeout = activityTimeout;
		this.initParameters();
		this.isBidirectional = true;		
		firstPacket(packet);
	}

	public Boolean hasSameDirection(PacketInfo packet) {
		return Arrays.equals(this.src, packet.getSrc());
	}
	
	public void initParameters(){
		this.forward = new ArrayList<PacketInfo>();
		this.backward = new ArrayList<PacketInfo>();
		this.flowIAT = new SummaryStatistics();
		this.forwardIAT = new SummaryStatistics();
		this.backwardIAT = new SummaryStatistics();
		this.flowActive = new SummaryStatistics();
		this.flowIdle = new SummaryStatistics();
		this.flowLengthStats = new SummaryStatistics();
		this.fwdPktStats = new SummaryStatistics();
		this.bwdPktStats =  new SummaryStatistics();
		this.flagCounts = new HashMap<String, MutableInt>();
		initFlags();
		this.forwardBytes = 0L;
		this.backwardBytes = 0L;	
		this.startActiveTime = 0L;
		this.endActiveTime = 0L;
		this.src = null;
		this.dst = null;
		this.fPSH_cnt=0;
		this.bPSH_cnt=0;
		this.fURG_cnt=0;
		this.bURG_cnt=0;
		this.fFIN_cnt=0;
		this.bFIN_cnt=0;		
		this.fHeaderBytes=0L;
		this.bHeaderBytes=0L;

	}
	
	
	public void firstPacket(PacketInfo packet){
		updateFlowBulk(packet);
		detectUpdateSubflows(packet);
		checkFlags(packet);
		this.flowStartTime = packet.getTimeStamp();
		this.flowLastSeen = packet.getTimeStamp();
		this.startActiveTime = packet.getTimeStamp();
		this.endActiveTime = packet.getTimeStamp();
		this.flowLengthStats.addValue((double)packet.getPayloadBytes());

		if(this.src==null){
			this.src = packet.getSrc();
			this.srcPort = packet.getSrcPort();
		}
		if(this.dst==null){
			this.dst = packet.getDst();
			this.dstPort = packet.getDstPort();
		}		
		if(Arrays.equals(this.src, packet.getSrc())){
			this.min_seg_size_forward = packet.getHeaderBytes();
			Init_Win_bytes_forward = packet.getTCPWindow();
			this.flowLengthStats.addValue((double)packet.getPayloadBytes());
			this.fwdPktStats.addValue((double)packet.getPayloadBytes());
			this.fHeaderBytes = packet.getHeaderBytes();
			this.forwardLastSeen = packet.getTimeStamp();
			this.forwardBytes+=packet.getPayloadBytes();
			this.forward.add(packet);
			if(packet.isFlagPSH()){
				this.fPSH_cnt++;
			}
			if(packet.isFlagURG()){
				this.fURG_cnt++;
			}
		}else{
			Init_Win_bytes_backward = packet.getTCPWindow();
			this.flowLengthStats.addValue((double)packet.getPayloadBytes());
			this.bwdPktStats.addValue((double)packet.getPayloadBytes());
			this.bHeaderBytes = packet.getHeaderBytes();
			this.backwardLastSeen = packet.getTimeStamp();
			this.backwardBytes+=packet.getPayloadBytes();
			this.backward.add(packet);
			if(packet.isFlagPSH()){
				this.bPSH_cnt++;
			}
			if(packet.isFlagURG()){
				this.bURG_cnt++;
			}
		}
		this.protocol = packet.getProtocol();
		this.flowId = packet.getFlowId();		
	}
    
    public void addPacket(PacketInfo packet){
		updateFlowBulk(packet);
		detectUpdateSubflows(packet);
		checkFlags(packet);
    	long currentTimestamp = packet.getTimeStamp();
    	if(isBidirectional){
			this.flowLengthStats.addValue((double)packet.getPayloadBytes());

    		if(Arrays.equals(this.src, packet.getSrc())){
				if(packet.getPayloadBytes() >=1){
					this.Act_data_pkt_forward++;
				}
				System.out.println("Adding Fwd Packet for " + this.flowId + ". Current N" + ((Long) this.fwdPktStats.getN()).toString());
				this.fwdPktStats.addValue((double)packet.getPayloadBytes());
				System.out.println("Result of adding Fwd Packet for " + this.flowId + ". Current N" + ((Long) this.fwdPktStats.getN()).toString());
				this.fHeaderBytes +=packet.getHeaderBytes();
    			this.forward.add(packet);   
    			this.forwardBytes+=packet.getPayloadBytes();
    			if (this.forward.size()>1)
    				this.forwardIAT.addValue(currentTimestamp -this.forwardLastSeen);
    			this.forwardLastSeen = currentTimestamp;
				this.min_seg_size_forward = Math.min(packet.getHeaderBytes(),this.min_seg_size_forward);

    		}else{
				System.out.println("Adding Bwd Packet for " + this.flowId + ". Current N" + ((Long) this.bwdPktStats.getN()).toString());
				this.bwdPktStats.addValue((double)packet.getPayloadBytes());
				System.out.println("Result of adding Bwd Packet for " + this.flowId + ". Current N" + ((Long) this.bwdPktStats.getN()).toString());
				Init_Win_bytes_backward = packet.getTCPWindow();
				this.bHeaderBytes+=packet.getHeaderBytes();
    			this.backward.add(packet);
    			this.backwardBytes+=packet.getPayloadBytes();
    			if (this.backward.size()>1)
    				this.backwardIAT.addValue(currentTimestamp-this.backwardLastSeen);
    			this.backwardLastSeen = currentTimestamp;
    		}
    	} else {
			if(packet.getPayloadBytes() >=1) {
				this.Act_data_pkt_forward++;
			}
			this.fwdPktStats.addValue((double)packet.getPayloadBytes());
			this.flowLengthStats.addValue((double)packet.getPayloadBytes());
			this.fHeaderBytes +=packet.getHeaderBytes();
    		this.forward.add(packet);    		
    		this.forwardBytes+=packet.getPayloadBytes();
    		this.forwardIAT.addValue(currentTimestamp-this.forwardLastSeen);
    		this.forwardLastSeen = currentTimestamp;
			this.min_seg_size_forward = Math.min(packet.getHeaderBytes(),this.min_seg_size_forward);
    	}

    	this.flowIAT.addValue(packet.getTimeStamp()-this.flowLastSeen);
    	this.flowLastSeen = packet.getTimeStamp();
    	
    }

	public double getfPktsPerSecond(){
		long duration = this.flowLastSeen - this.flowStartTime;
		if(duration > 0){
			return (this.forward.size()/((double)duration/1000000L));
		}
		else
			return 0;
	}
	public double getbPktsPerSecond(){
		long duration = this.flowLastSeen - this.flowStartTime;
		if(duration > 0){
			return (this.backward.size()/((double)duration/1000000L));
		}
		else
			return 0;
	}

	public double getDownUpRatio(){
		if(this.forward.size() > 0){
			return (double)(this.backward.size()/this.forward.size());
		}
		return 0;
	}

	public double getAvgPacketSize(){
		if(this.packetCount() > 0){
			return (this.flowLengthStats.getSum()/this.packetCount());
		}
		return 0;
	}

	public double fAvgSegmentSize(){
		if (this.forward.size()!=0)
			return (this.fwdPktStats.getSum() / (double)this.forward.size());
		return 0;
	}

	public double bAvgSegmentSize(){
		if (this.backward.size()!=0)
			return (this.bwdPktStats.getSum() / (double)this.backward.size());
		return 0;
	}

    public void initFlags(){
		flagCounts.put("FIN", new MutableInt());
		flagCounts.put("SYN", new MutableInt());
		flagCounts.put("RST", new MutableInt());
		flagCounts.put("PSH", new MutableInt());
		flagCounts.put("ACK", new MutableInt());
		flagCounts.put("URG", new MutableInt());
		flagCounts.put("CWR", new MutableInt());
		flagCounts.put("ECE", new MutableInt());
	}

	public void checkFlags(PacketInfo packet){
		if(packet.isFlagFIN()){
			flagCounts.get("FIN").increment();
		}
		if(packet.isFlagSYN()){
			flagCounts.get("SYN").increment();
		}
		if(packet.isFlagRST()){
			flagCounts.get("RST").increment();
		}
		if(packet.isFlagPSH()){
			flagCounts.get("PSH").increment();
		}
		if(packet.isFlagACK()){
			flagCounts.get("ACK").increment();
		}
		if(packet.isFlagURG()){
			flagCounts.get("URG").increment();
		}
		if(packet.isFlagCWR()){
			flagCounts.get("CWR").increment();
		}
		if(packet.isFlagECE()){
			flagCounts.get("ECE").increment();
		}
	}




	public long getSflow_fbytes(){
		if(sfCount <= 0) return 0;
		return this.forwardBytes/sfCount;
	}

	public long getSflow_fpackets(){
		if(sfCount <= 0) return 0;
		return this.forward.size()/sfCount;
	}

	public long getSflow_bbytes(){
		if(sfCount <= 0) return 0;
		return this.backwardBytes/sfCount;
	}
	public long getSflow_bpackets(){
		if(sfCount <= 0) return 0;
		return this.backward.size()/sfCount;
	}

	private long sfLastPacketTS=-1;
	private int sfCount=0;

	void detectUpdateSubflows( PacketInfo packet ){
		if(sfLastPacketTS == -1){
			sfLastPacketTS = packet.getTimeStamp();
		}
		if(((packet.getTimeStamp() - sfLastPacketTS)/(double)1000000)  > 1.0){
			sfCount ++ ;
			updateActiveIdleTime(packet.getTimeStamp(), this.activityTimeout);
		}
		sfLastPacketTS = packet.getTimeStamp() ;
	}

	//////////////////////////////
	private long fbulkDuration=0;
	private long fbulkPacketCount=0;
	private long fbulkSizeTotal=0;
	private long fbulkStateCount=0;
	private long fbulkPacketCountHelper=0;
	private long fbulkStartHelper=0;
	private long fbulkSizeHelper=0;
	private long flastBulkTS=0;
	private long bbulkDuration=0;
	private long bbulkPacketCount=0;
	private long bbulkSizeTotal=0;
	private long bbulkStateCount=0;
	private long bbulkPacketCountHelper=0;
	private long bbulkStartHelper=0;
	private long bbulkSizeHelper=0;
	private long blastBulkTS=0;


	public void updateFlowBulk (PacketInfo packet){

		if(this.src == packet.getSrc()){
			updateForwardBulk(packet,blastBulkTS);
		}else {
			updateBackwardBulk(packet,flastBulkTS);
		}

	}

	public void updateForwardBulk(PacketInfo packet, long tsOflastBulkInOther){

		long size=packet.getPayloadBytes();
		if (tsOflastBulkInOther > fbulkStartHelper) fbulkStartHelper = 0;
		if (size <= 0) return ;

		packet.setPayloadPacket(packet.getPayloadPacket() + 1);
		// packet.getPayloadPacket()

		if (fbulkStartHelper == 0){
			fbulkStartHelper = packet.getTimeStamp();
			fbulkPacketCountHelper = 1;
			fbulkSizeHelper = size ;
			flastBulkTS = packet.getTimeStamp();
		} //possible bulk
		else{
			// Too much idle time?
			if (((packet.getTimeStamp() - flastBulkTS)/(double)1000000) > 1.0){
				fbulkStartHelper = packet.getTimeStamp();
				flastBulkTS = packet.getTimeStamp();
				fbulkPacketCountHelper = 1;
				fbulkSizeHelper = size;
			}// Add to bulk
			else{
				fbulkPacketCountHelper += 1;
				fbulkSizeHelper        += size ;
				//New bulk
				if (fbulkPacketCountHelper == 4){
					fbulkStateCount  += 1;
					fbulkPacketCount += fbulkPacketCountHelper;
					fbulkSizeTotal   += fbulkSizeHelper;
					fbulkDuration    += packet.getTimeStamp() - fbulkStartHelper;
				} //Continuation of existing bulk
				else if (fbulkPacketCountHelper > 4){
					fbulkPacketCount += 1;
					fbulkSizeTotal   += size;
					fbulkDuration    += packet.getTimeStamp() - flastBulkTS;
				}
				flastBulkTS = packet.getTimeStamp();
			}
		}
	}

	public void updateBackwardBulk(PacketInfo packet , long tsOflastBulkInOther){
		/*bAvgBytesPerBulk =0;
		bbulkSizeTotal=0;
		bbulkStateCount=0;*/
		long size=packet.getPayloadBytes();
		if (tsOflastBulkInOther > bbulkStartHelper) bbulkStartHelper = 0;
		if ( size<= 0) return ;

		packet.setPayloadPacket(packet.getPayloadPacket() + 1);
		// packet.getPayloadPacket()

		if ( bbulkStartHelper == 0 ){
			bbulkStartHelper = packet.getTimeStamp();
			bbulkPacketCountHelper = 1;
			bbulkSizeHelper = size ;
			blastBulkTS = packet.getTimeStamp();
		} //possible bulk
		else{
			// Too much idle time?
			if (((packet.getTimeStamp() - blastBulkTS)/(double)1000000) > 1.0){
				bbulkStartHelper = packet.getTimeStamp();
				blastBulkTS = packet.getTimeStamp();
				bbulkPacketCountHelper = 1;
				bbulkSizeHelper = size;
			}// Add to bulk
			else{
				bbulkPacketCountHelper += 1;
				bbulkSizeHelper += size ;
				//New bulk
				if (bbulkPacketCountHelper == 4){
					bbulkStateCount  += 1;
					bbulkPacketCount += bbulkPacketCountHelper;
					bbulkSizeTotal   += bbulkSizeHelper;
					bbulkDuration    += packet.getTimeStamp() - bbulkStartHelper;
				} //Continuation of existing bulk
				else if (bbulkPacketCountHelper > 4){
					bbulkPacketCount += 1;
					bbulkSizeTotal   += size;
					bbulkDuration    += packet.getTimeStamp() - blastBulkTS;
				}
				blastBulkTS = packet.getTimeStamp();
			}
		}

	}

	public  long fbulkStateCount() {
		return fbulkStateCount;
	}

	public  long fbulkSizeTotal() {
		return fbulkSizeTotal;
	}

	public long fbulkPacketCount() {
		return fbulkPacketCount;
	}

	public long fbulkDuration() {
		return fbulkDuration;
	}
	public double fbulkDurationInSecond() {
		return fbulkDuration/(double)1000000;
	}



	//Client average bytes per bulk
	public long fAvgBytesPerBulk(){
		if (this.fbulkStateCount() != 0 )
			return (this.fbulkSizeTotal() / this.fbulkStateCount());
		return 0;
	}


	//Client average packets per bulk
	public long fAvgPacketsPerBulk(){
		if (this.fbulkStateCount() != 0 )
			return (this.fbulkPacketCount() / this.fbulkStateCount());
		return 0;
	}


	//Client average bulk rate
	public long fAvgBulkRate(){
		if (this.fbulkDuration() != 0 )
			return (long)(this.fbulkSizeTotal() / this.fbulkDurationInSecond());
		return 0;
	}


	//new features server
	public long bbulkPacketCount() {
		return bbulkPacketCount;
	}

	public long bbulkStateCount() {
		return bbulkStateCount;
	}

	public long bbulkSizeTotal() {
		return bbulkSizeTotal;
	}

	public long bbulkDuration() {
		return bbulkDuration;
	}
	public double bbulkDurationInSecond() {
		return bbulkDuration/(double)1000000;
	}

	//Server average bytes per bulk
	public long bAvgBytesPerBulk(){
		if(this.bbulkStateCount() != 0)
			return (this.bbulkSizeTotal() /  this.bbulkStateCount());
		return 0;
	}

	//Server average packets per bulk
	public long bAvgPacketsPerBulk(){
		if(this.bbulkStateCount() != 0 )
			return (this.bbulkPacketCount() /  this.bbulkStateCount());
		return 0;
	}
	//Server average bulk rate
	public long bAvgBulkRate(){
		if(this.bbulkDuration() != 0)
			return (long)(this.bbulkSizeTotal() / this.bbulkDurationInSecond());
		return 0;
	}

	////////////////////////////


    public void updateActiveIdleTime(long currentTime, long threshold){
    	if ((currentTime - this.endActiveTime) > threshold){
    		if((this.endActiveTime - this.startActiveTime) > 0){
	      		this.flowActive.addValue(this.endActiveTime - this.startActiveTime);	      		
    		}
    		this.flowIdle.addValue(currentTime - this.endActiveTime);
    		this.startActiveTime = currentTime;
    		this.endActiveTime = currentTime;
    	}else{
    		this.endActiveTime = currentTime;
    	}
    }
    
    public void endActiveIdleTime(long currentTime, long threshold, long flowTimeOut, boolean isFlagEnd){
		
    	if((this.endActiveTime - this.startActiveTime) > 0){
      		this.flowActive.addValue(this.endActiveTime - this.startActiveTime);	      		
		}
    	
    	if (!isFlagEnd && ((flowTimeOut - (this.endActiveTime-this.flowStartTime))>0)){
    		this.flowIdle.addValue(flowTimeOut - (this.endActiveTime-this.flowStartTime));
    	}
    }
    
    public int packetCount(){
    	if(isBidirectional){
    		return (this.forward.size() + this.backward.size()); 
    	}else{
    		return this.forward.size();    		
    	}
    }
    
	public List<PacketInfo> getForward() {
		return new ArrayList<>(forward);
	}

	public void setForward(List<PacketInfo> forward) {
		this.forward = forward;
	}

	public List<PacketInfo> getBackward() {
		return new ArrayList<>(backward);
	}

	public void setBackward(List<PacketInfo> backward) {
		this.backward = backward;
	}

	public boolean isBidirectional() {
		return isBidirectional;
	}

	public void setBidirectional(boolean isBidirectional) {
		this.isBidirectional = isBidirectional;
	}

	public byte[] getSrc() {
		return Arrays.copyOf(src,src.length);
	}

	public void setSrc(byte[] src) {
		this.src = src;
	}

	public byte[] getDst() {
		return Arrays.copyOf(dst,dst.length);
	}

	public void setDst(byte[] dst) {
		this.dst = dst;
	}

	public int getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(int srcPort) {
		this.srcPort = srcPort;
	}

	public int getDstPort() {
		return dstPort;
	}

	public void setDstPort(int dstPort) {
		this.dstPort = dstPort;
	}

	public int getProtocol() {
		return protocol;
	}
	
	public String getProtocolStr() {
		switch(this.protocol){
		case(6):
			return "TCP";
		case(17):
		    return "UDP";
		}
		return "UNKNOWN";
	}	

	public void setProtocol(int protocol) {
		this.protocol = protocol;
	}

	public long getFlowStartTime() {
		return flowStartTime;
	}

	public void setFlowStartTime(long flowStartTime) {
		this.flowStartTime = flowStartTime;
	}

	public String getFlowId() {
		return flowId;
	}

	public void setFlowId(String flowId) {
		this.flowId = flowId;
	}

	public long getLastSeen() {
		return flowLastSeen;
	}

	public void setLastSeen(long lastSeen) {
		this.flowLastSeen = lastSeen;
	}

	public long getStartActiveTime() {
		return startActiveTime;
	}

	public void setStartActiveTime(long startActiveTime) {
		this.startActiveTime = startActiveTime;
	}

	public long getEndActiveTime() {
		return endActiveTime;
	}

	public void setEndActiveTime(long endActiveTime) {
		this.endActiveTime = endActiveTime;
	}
		
	public String getSrcIP() {
		return PacketUtils.byteArrayToIp(src);
	}
	
	public String getDstIP() {
		return PacketUtils.byteArrayToIp(dst);
	}
	
	public String getTimeStamp() {
		return DateUtils.parseDateFromLong(flowStartTime/1000L, "dd/MM/yyyy hh:mm:ss");
	}
	
	public long getFlowDuration() {
		return flowLastSeen - flowStartTime;
	}
	
	public long getTotalFwdPackets() {
		return fwdPktStats.getN();
	}
	
	public long getTotalBackwardPackets() {
		return bwdPktStats.getN();
	}
	
	public double getTotalLengthofFwdPackets() {
		return fwdPktStats.getSum();
	}
	
	public double getTotalLengthofBwdPackets() {
		return bwdPktStats.getSum();
	}
	
	public double getFwdPacketLengthMax() {
		return (fwdPktStats.getN() > 0L)? fwdPktStats.getMax():0;
	}
	
	public double getFwdPacketLengthMin() {
		return (fwdPktStats.getN() > 0L)? fwdPktStats.getMin():0;
	}
	
	public double getFwdPacketLengthMean() {
		return (fwdPktStats.getN() > 0L)? fwdPktStats.getMean():0;
	}
	
	public double getFwdPacketLengthStd() {
		return (fwdPktStats.getN() > 0L)? fwdPktStats.getStandardDeviation():0;
	}
	
	public double getBwdPacketLengthMax() {
		return (bwdPktStats.getN() > 0L)? bwdPktStats.getMax():0;
	}
	
	public double getBwdPacketLengthMin() {
		return (bwdPktStats.getN() > 0L)? bwdPktStats.getMin():0;
	}
	
	public double getBwdPacketLengthMean() {
		return (bwdPktStats.getN() > 0L)? bwdPktStats.getMean():0;
	}
	
	public double getBwdPacketLengthStd() {
		return (bwdPktStats.getN() > 0L)? bwdPktStats.getStandardDeviation():0;
	}
	
	public double getFlowBytesPerSec(){
		//flow duration is in microseconds, therefore packets per seconds = packets / (duration/1000000)
		return ((double)(forwardBytes+backwardBytes))/((double)getFlowDuration()/1000000L);
	}
	
	public double getFlowPacketsPerSec() {
		return ((double)packetCount())/((double)getFlowDuration()/1000000L);
	}
	
	public SummaryStatistics getFlowIAT() {
		return flowIAT;
	}
	
	public double getFwdIATTotal() {
		return (forward.size()>1)? forwardIAT.getSum():0;
	}
	
	public double getFwdIATMean() {
		return (forward.size()>1)? forwardIAT.getMean():0;
	}
	
	public double getFwdIATStd() {
		return (forward.size()>1)? forwardIAT.getStandardDeviation():0;
	}
	
	public double getFwdIATMax() {
		return (forward.size()>1)? forwardIAT.getMax():0;
	}
	
	public double getFwdIATMin() {
		return (forward.size()>1)? forwardIAT.getMin():0;
	}
	
	public double getBwdIATTotal() {
		return (backward.size()>1)? backwardIAT.getSum():0;
	}
	
	public double getBwdIATMean() {
		return (backward.size()>1)? backwardIAT.getMean():0;
	}
	
	public double getBwdIATStd() {
		return (backward.size()>1)? backwardIAT.getStandardDeviation():0;
	}
	
	public double getBwdIATMax() {
		return (backward.size()>1)? backwardIAT.getMax():0;
	}
	
	public double getBwdIATMin() {
		return (backward.size()>1)? backwardIAT.getMin():0;
	}
	
	public int getFwdPSHFlags() {
		return fPSH_cnt;
	}
	
	public int getBwdPSHFlags() {
		return bPSH_cnt;
	}
	
	public int getFwdURGFlags() {
		return fURG_cnt;
	}
	
	public int getBwdURGFlags() {
		return bURG_cnt;
	}
	
	public int getFwdFINFlags() {
		return fFIN_cnt;
	}
	
	public int getBwdFINFlags() {
		return bFIN_cnt;
	}
	
	public int setFwdFINFlags() {
		fFIN_cnt++;
		return fFIN_cnt;
	}
	
	public int setBwdFINFlags() {
		bFIN_cnt++;
		return bFIN_cnt;
	}	
	
	public long getFwdHeaderLength() {
		return fHeaderBytes;
	}
	
	public long getBwdHeaderLength() {
		return bHeaderBytes;
	}
	
	public double getMinPacketLength() {
		return (forward.size() > 0 || backward.size() > 0)? flowLengthStats.getMin():0;
	}
	
	public double getMaxPacketLength() {
		return (forward.size() > 0 || backward.size() > 0)? flowLengthStats.getMax():0;
	}
	
	public double getPacketLengthMean() {
		return (forward.size() > 0 || backward.size() > 0)? flowLengthStats.getMean():0;
	}
	
	public double getPacketLengthStd() {
		return (forward.size() > 0 || backward.size() > 0)? flowLengthStats.getStandardDeviation():0;
	}
	
	public double getPacketLengthVariance() {
		return (forward.size() > 0 || backward.size() > 0)? flowLengthStats.getVariance():0;
	}
	
	public int getFlagCount(String key) {
		return flagCounts.get(key).getValue();
	}
	
	public int getInit_Win_bytes_forward() {
		return Init_Win_bytes_forward;
	}
	
	public int getInit_Win_bytes_backward() {
		return Init_Win_bytes_backward;
	}
	
	public long getAct_data_pkt_forward() {
		return Act_data_pkt_forward;
	}
	
	public long getmin_seg_size_forward() {
		return min_seg_size_forward;
	}
	
	public double getActiveMean() {
		return (flowActive.getN()>0)? flowActive.getMean():0;
	} 
	
	public double getActiveStd() {
		return (flowActive.getN()>0)? flowActive.getStandardDeviation():0;
	}
	
	public double getActiveMax() {
		return (flowActive.getN()>0)? flowActive.getMax():0;
	}
	
	public double getActiveMin() {
		return (flowActive.getN()>0)? flowActive.getMin():0;
	}
	
	public double getIdleMean() {
		return (flowIdle.getN()>0)? flowIdle.getMean():0;
	}
	
	public double getIdleStd() {
		return (flowIdle.getN()>0)? flowIdle.getStandardDeviation():0;
	}
	
	public double getIdleMax() {
		return (flowIdle.getN()>0)? flowIdle.getMax():0;
	}
	
	public double getIdleMin() {
		return (flowIdle.getN()>0)? flowIdle.getMin():0;
	}
	
	public String getLabel() {
        return "NeedManualLabel";
    }
	
    public String dumpFlowBasedFeaturesEx() {
    	StringBuilder dump = new StringBuilder();
    	
    	dump.append(flowId).append(separator);                						//1
    	dump.append(PacketUtils.byteArrayToIp(src)).append(separator);   						//2
    	dump.append(getSrcPort()).append(separator);          						//3
    	dump.append(PacketUtils.byteArrayToIp(dst)).append(separator);  						//4
    	dump.append(getDstPort()).append(separator);          						//5
    	dump.append(getProtocol()).append(separator);         						//6 
    	
    	String starttime = DateUtils.convertMilliseconds2String(flowStartTime/1000L, "dd/MM/yyyy hh:mm:ss a");
    	dump.append(starttime).append(separator);									//7
    	
    	long flowDuration = flowLastSeen - flowStartTime;
    	dump.append(flowDuration).append(separator);								//8
    	
    	dump.append(fwdPktStats.getN()).append(separator);							//9
    	dump.append(bwdPktStats.getN()).append(separator);							//10	
    	dump.append(fwdPktStats.getSum()).append(separator);						//11
    	dump.append(bwdPktStats.getSum()).append(separator);						//12
    	
    	if(fwdPktStats.getN() > 0L) {
    		dump.append(fwdPktStats.getMax()).append(separator);					//13
    		dump.append(fwdPktStats.getMin()).append(separator);					//14
    		dump.append(fwdPktStats.getMean()).append(separator);					//15
    		dump.append(fwdPktStats.getStandardDeviation()).append(separator);		//16
    	}else {
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    	}
    	
    	if(bwdPktStats.getN() > 0L) {
    		dump.append(bwdPktStats.getMax()).append(separator);					//17
    		dump.append(bwdPktStats.getMin()).append(separator);					//18
    		dump.append(bwdPktStats.getMean()).append(separator);					//19
    		dump.append(bwdPktStats.getStandardDeviation()).append(separator);		//20
		}else{
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
		}
    	dump.append(((double)(forwardBytes+backwardBytes))/((double)flowDuration/1000000L)).append(separator);//21
    	dump.append(((double)packetCount())/((double)flowDuration/1000000L)).append(separator);//22
    	dump.append(flowIAT.getMean()).append(separator);							//23
    	dump.append(flowIAT.getStandardDeviation()).append(separator);				//24
    	dump.append(flowIAT.getMax()).append(separator);							//25
    	dump.append(flowIAT.getMin()).append(separator);							//26
    	
    	if(this.forward.size()>1){
        	dump.append(forwardIAT.getSum()).append(separator);						//27
        	dump.append(forwardIAT.getMean()).append(separator);					//28
        	dump.append(forwardIAT.getStandardDeviation()).append(separator);		//29	
        	dump.append(forwardIAT.getMax()).append(separator);						//30
        	dump.append(forwardIAT.getMin()).append(separator);						//31
        	
    	}else{
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    	}
    	if(this.backward.size()>1){
        	dump.append(backwardIAT.getSum()).append(separator);					//32
        	dump.append(backwardIAT.getMean()).append(separator);					//33
        	dump.append(backwardIAT.getStandardDeviation()).append(separator);		//34	
        	dump.append(backwardIAT.getMax()).append(separator);					//35
        	dump.append(backwardIAT.getMin()).append(separator);					//36
    	}else{
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    	}
    	
		dump.append(fPSH_cnt).append(separator);									//37
		dump.append(bPSH_cnt).append(separator);									//38
		dump.append(fURG_cnt).append(separator);									//39
		dump.append(bURG_cnt).append(separator);									//40

		dump.append(fHeaderBytes).append(separator);								//41
		dump.append(bHeaderBytes).append(separator);								//42
		dump.append(getfPktsPerSecond()).append(separator);							//43
		dump.append(getbPktsPerSecond()).append(separator);							//44
		
		
		if(this.forward.size() > 0 || this.backward.size() > 0){
			dump.append(flowLengthStats.getMin()).append(separator);				//45
			dump.append(flowLengthStats.getMax()).append(separator);				//46
			dump.append(flowLengthStats.getMean()).append(separator);				//47
			dump.append(flowLengthStats.getStandardDeviation()).append(separator);	//48
			dump.append(flowLengthStats.getVariance()).append(separator);			//49
		}else{//seem to less one
			dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
		}
		
		/*for(MutableInt v:flagCounts.values()) {
			dump.append(v).append(separator);
		}
		for(String key: flagCounts.keySet()){
			dump.append(flagCounts.get(key).value).append(separator);				//50,51,52,53,54,55,56,57
		} */
		dump.append(flagCounts.get("FIN").getValue()).append(separator);                 //50
		dump.append(flagCounts.get("SYN").getValue()).append(separator);                 //51
		dump.append(flagCounts.get("RST").getValue()).append(separator);                  //52
		dump.append(flagCounts.get("PSH").getValue()).append(separator);                  //53
		dump.append(flagCounts.get("ACK").getValue()).append(separator);                  //54
		dump.append(flagCounts.get("URG").getValue()).append(separator);                  //55
		dump.append(flagCounts.get("CWR").getValue()).append(separator);                  //56
		dump.append(flagCounts.get("ECE").getValue()).append(separator);                  //57
		
		dump.append(getDownUpRatio()).append(separator);							//58
		dump.append(getAvgPacketSize()).append(separator);							//59
		dump.append(fAvgSegmentSize()).append(separator);							//60
		dump.append(bAvgSegmentSize()).append(separator);							//61
		//dump.append(fHeaderBytes).append(separator);								//62 dupicate with 41
		
		dump.append(fAvgBytesPerBulk()).append(separator);							//63	
		dump.append(fAvgPacketsPerBulk()).append(separator);						//64
		dump.append(fAvgBulkRate()).append(separator);								//65
		dump.append(bAvgBytesPerBulk()).append(separator);							//66
		dump.append(bAvgPacketsPerBulk()).append(separator);						//67
		dump.append(bAvgBulkRate()).append(separator);								//68
    	
		dump.append(getSflow_fpackets()).append(separator);							//69
		dump.append(getSflow_fbytes()).append(separator);							//70
		dump.append(getSflow_bpackets()).append(separator);							//71
		dump.append(getSflow_bbytes()).append(separator);							//72
			
    	dump.append(Init_Win_bytes_forward).append(separator);						//73
    	dump.append(Init_Win_bytes_backward).append(separator);						//74
    	dump.append(Act_data_pkt_forward).append(separator);						//75
    	dump.append(min_seg_size_forward).append(separator);						//76
    	
    	
    	if(this.flowActive.getN()>0){
        	dump.append(flowActive.getMean()).append(separator);					//77
        	dump.append(flowActive.getStandardDeviation()).append(separator);		//78
        	dump.append(flowActive.getMax()).append(separator);						//79
        	dump.append(flowActive.getMin()).append(separator);						//80
    	}else{
			dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    	}    	
    	
    	if(this.flowIdle.getN()>0){
	    	dump.append(flowIdle.getMean()).append(separator);						//81
	    	dump.append(flowIdle.getStandardDeviation()).append(separator);			//82
	    	dump.append(flowIdle.getMax()).append(separator);						//83
	    	dump.append(flowIdle.getMin()).append(separator);						//84	
    	}else{
			dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    		dump.append(0).append(separator);
    	}

        dump.append(getLabel());

    	
    	return dump.toString();
    }
}
