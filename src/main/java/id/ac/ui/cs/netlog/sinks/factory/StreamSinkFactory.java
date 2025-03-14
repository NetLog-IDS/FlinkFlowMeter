package id.ac.ui.cs.netlog.sinks.factory;

import org.apache.flink.api.java.utils.ParameterTool;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.sinks.KafkaStreamSink;
import id.ac.ui.cs.netlog.sinks.PrintStreamSink;
import id.ac.ui.cs.netlog.sinks.StreamSink;
import id.ac.ui.cs.netlog.sinks.enums.StreamSinkEnum;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StreamSinkFactory {
    private final ObjectMapper objectMapper;

    public StreamSink getStreamSink(StreamSinkEnum sinkEnum, ParameterTool parameterTool) {
        if (sinkEnum.equals(StreamSinkEnum.PRINT)) {
            return new PrintStreamSink();
        } else {
            return new KafkaStreamSink(parameterTool, objectMapper);
        }
    }
}
