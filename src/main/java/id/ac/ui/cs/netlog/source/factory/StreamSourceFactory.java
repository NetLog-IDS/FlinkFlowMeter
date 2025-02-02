package id.ac.ui.cs.netlog.source.factory;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.source.KafkaStreamSource;
import id.ac.ui.cs.netlog.source.SocketStreamSource;
import id.ac.ui.cs.netlog.source.StreamSource;
import id.ac.ui.cs.netlog.source.enums.StreamSourceEnum;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StreamSourceFactory {
    private final ObjectMapper objectMapper;

    public StreamSource getStreamSource(StreamSourceEnum sourceEnum, StreamExecutionEnvironment env, ParameterTool parameterTool) {
        if (sourceEnum.equals(StreamSourceEnum.KAFKA)) {
            return new KafkaStreamSource(env, parameterTool, objectMapper);
        } else {
            return new SocketStreamSource(env, parameterTool, objectMapper);
        }
    }
}
