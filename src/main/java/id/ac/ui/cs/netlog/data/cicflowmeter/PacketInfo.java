package id.ac.ui.cs.netlog.data.cicflowmeter;

import java.util.Arrays;

import id.ac.ui.cs.netlog.utils.PacketUtils;
import lombok.Data;

@Data
public class PacketInfo {
	private String packetId;
	private long order;
	private long arrivalTime;
	private long sniffTime;
	private String publisherId;

    private byte[] src;
    private byte[] dst;
    private int srcPort;
    private int dstPort;
    private ProtocolEnum protocol = ProtocolEnum.DEFAULT;
    private long timeStamp;
    private long payloadBytes;
    private String flowId = null;  
    private boolean flagFIN = false;
	private boolean flagPSH = false;
	private boolean flagURG = false;
	private boolean flagECE = false;
	private boolean flagSYN = false;
	private boolean flagACK = false;
	private boolean flagCWR = false;
	private boolean flagRST = false;
	private	int TCPWindow=0;
	private	long headerBytes;
	private int payloadPacket=0;
	private int icmpCode = -1;
	private int icmpType = -1;

	private TCPRetransmission tcpRetransmission;

	public PacketInfo(byte[] src, byte[] dst, int srcPort, int dstPort,
			ProtocolEnum protocol, long timeStamp) {
		this.src = src;
		this.dst = dst;
		this.srcPort = srcPort;
		this.dstPort = dstPort;
		this.protocol = protocol;
		this.timeStamp = timeStamp;
		generateFlowId();
	}
	
    public PacketInfo() {
		// TODO: Check if generateFlowId needs to be done
	}

	public static PacketInfo getOrderComparator(Long order) {
		PacketInfo packet = new PacketInfo();
		packet.setOrder(order);
		return packet;
	}

	public static PacketInfo getTimestampComparatorUpperBound(Long timestamp) {
		PacketInfo packet = new PacketInfo();
		packet.setPacketId("~~~~~~~~-~~~~-~~~~-~~~~-~~~~~~~~~~~~");
		packet.setTimeStamp(timestamp);
		return packet;
	}

	public static PacketInfo getArrivalComparatorUpperBound(Long timestamp) {
		PacketInfo packet = new PacketInfo();
		packet.setPacketId("~~~~~~~~-~~~~-~~~~-~~~~-~~~~~~~~~~~~");
		packet.setArrivalTime(timestamp);
		return packet;
	}

	public String generateFlowId(){
    	boolean forward = true;
    	
    	for(int i=0; i<this.src.length;i++){           
    		if(((Byte)(this.src[i])).intValue() != ((Byte)(this.dst[i])).intValue()){
    			if(((Byte)(this.src[i])).intValue() > ((Byte)(this.dst[i])).intValue()){
    				forward = false;
    			}
    			i=this.src.length;
    		}
    	}
    	
        if(forward){
            this.flowId = this.getSourceIP() + "-" + this.getDestinationIP() + "-" + this.srcPort  + "-" + this.dstPort  + "-" + this.protocol.val;
        }else{
            this.flowId = this.getDestinationIP() + "-" + this.getSourceIP() + "-" + this.dstPort  + "-" + this.srcPort  + "-" + this.protocol.val;
        }
        return this.flowId;
	}

	private String getNormalId() {
		return this.getSourceIP() + "-" + this.getDestinationIP() + "-" + this.srcPort  + "-" + this.dstPort  + "-" + this.protocol.val;
	}

	private String getReverseId() {
		return this.getDestinationIP() + "-" + this.getSourceIP() + "-" + this.dstPort  + "-" + this.srcPort  + "-" + this.protocol.val;
	}

	public String getFlowBidirectionalId() {
		boolean forward = true;
    	for (int i = 0; i < this.src.length; i++) {   
			int srcByte = ((Byte)(this.src[i])).intValue();	      
			int dstByte = ((Byte)(this.dst[i])).intValue();
			if (srcByte != dstByte) {
				if (srcByte > dstByte) {
					forward = false;
				}
				break;
			}
    	}
		return (forward) ? getNormalId() : getReverseId();
	}

 	public String fwdFlowId() {  
		this.flowId = this.getSourceIP() + "-" + this.getDestinationIP() + "-" + this.srcPort  + "-" + this.dstPort  + "-" + this.protocol.val;
		return this.flowId;
	}
	
	public String bwdFlowId() {  
		this.flowId = this.getDestinationIP() + "-" + this.getSourceIP() + "-" + this.dstPort  + "-" + this.srcPort  + "-" + this.protocol.val;
		return this.flowId;
	}
    
	public String dumpInfo() {
		return null;
	}

	public Integer incrementPayloadPacket() {
		return payloadPacket += 1;
	}
    
    public String getSourceIP(){
		return PacketUtils.byteArrayToIp(this.src);
    	// return FormatUtils.ip(this.src);
    }

    public String getDestinationIP(){
		return PacketUtils.byteArrayToIp(this.dst);
    	// return FormatUtils.ip(this.dst);
    }
    
	public byte[] getSrc() {
		return Arrays.copyOf(src,src.length);
	}

	public byte[] getDst() {
		return Arrays.copyOf(dst,dst.length);
	}

	public String getFlowId() {
		return this.flowId != null ? this.flowId : generateFlowId();
	}

	public boolean isForwardPacket(byte[] sourceIP) {
		return Arrays.equals(sourceIP, this.src);
	}
}
