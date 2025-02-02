package id.ac.ui.cs.netlog.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.packets.Packet;
import id.ac.ui.cs.netlog.operators.PacketParser;

public class SocketStreamSource implements StreamSource {
    private static final String SOCKET_HOST = "source-host";
	private static final String SOCKET_PORT = "source-port";

    private final String host;
    private final Integer port;
	private final StreamExecutionEnvironment env;
	private final ObjectMapper objectMapper;

    public SocketStreamSource(StreamExecutionEnvironment env, ParameterTool parameters, ObjectMapper objectMapper) {
        this.host = parameters.get(SOCKET_HOST, "host.docker.internal");
        this.port = Integer.parseInt(parameters.get(SOCKET_PORT, "9999"));
		this.env = env;
		this.objectMapper = objectMapper;
    }

    public DataStream<Packet> getSourceStream() {
		return env.socketTextStream(host, port).map(new PacketParser(objectMapper));
	}
}
