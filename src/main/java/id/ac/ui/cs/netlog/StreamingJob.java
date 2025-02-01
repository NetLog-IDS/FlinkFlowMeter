package id.ac.ui.cs.netlog;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> stream = env.socketTextStream("host.docker.internal", 9999);
		stream.print();

		env.execute("FlinkFlowMeter");
    }
}
