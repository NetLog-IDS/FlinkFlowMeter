package id.ac.ui.cs.netlog.utils;

public class PacketUtils {
    public static byte[] ipToByteArray(String ip) {
        String[] parts = ip.split("\\.");
        byte[] ipBytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            ipBytes[i] = (byte) Integer.parseInt(parts[i]);
        }
        return ipBytes;
    }

    public static String byteArrayToIp(byte[] ipBytes) {
        StringBuilder ip = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            ip.append((ipBytes[i] & 0xFF));
            if (i < 3) {
                ip.append(".");
            }
        }
        return ip.toString();
    }

    public static Boolean ipLesserThan(byte[] ip1, byte[] ip2) {
        Boolean isLess = true;
        for(int i=0; i<ip1.length;i++){
    		if(((Byte)(ip1[i])).intValue() != ((Byte)(ip2[i])).intValue()){
    			if(((Byte)(ip1[i])).intValue() > ((Byte)(ip2[i])).intValue()){
    				isLess = false;
    			}
    			break;
    		}
    	}
        return isLess;
    }
}
