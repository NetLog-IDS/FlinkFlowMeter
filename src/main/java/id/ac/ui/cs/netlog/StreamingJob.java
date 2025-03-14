package id.ac.ui.cs.netlog;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;
import id.ac.ui.cs.netlog.modes.StreamMode;
import id.ac.ui.cs.netlog.modes.enums.ModeEnum;
import id.ac.ui.cs.netlog.modes.factory.ModeFactory;
import id.ac.ui.cs.netlog.sinks.StreamSink;
import id.ac.ui.cs.netlog.sinks.enums.StreamSinkEnum;
import id.ac.ui.cs.netlog.sinks.factory.StreamSinkFactory;
import id.ac.ui.cs.netlog.source.StreamSource;
import id.ac.ui.cs.netlog.source.enums.StreamSourceEnum;
import id.ac.ui.cs.netlog.source.factory.StreamSourceFactory;

public class StreamingJob {
	private static final String SOURCE_TYPE = "source";
	private static final String SINK_TYPE = "sink";
	private static final String MODE = "mode";

    public static void main(String[] args) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String sourceString = parameters.get(SOURCE_TYPE, StreamSourceEnum.SOCKET.toString());
		String sinkString = parameters.get(SINK_TYPE, StreamSinkEnum.PRINT.toString());
		String modeString = parameters.get(MODE, ModeEnum.ORDERED.toString());

		StreamSourceFactory sourceFactory = new StreamSourceFactory(objectMapper);
		StreamSource source = sourceFactory.getStreamSource(StreamSourceEnum.fromString(sourceString), env, parameters);

		ModeFactory modeFactory = new ModeFactory();
		StreamMode streamMode = modeFactory.getMode(ModeEnum.fromString(modeString));
		DataStream<FlowStats> stream = streamMode.createPipeline(source);

		StreamSinkFactory sinkFactory = new StreamSinkFactory(objectMapper);
		StreamSink streamSink = sinkFactory.getStreamSink(StreamSinkEnum.fromString(sinkString), parameters);
		streamSink.applySink(stream);

		env.execute("FlinkFlowMeter");
    }
}
