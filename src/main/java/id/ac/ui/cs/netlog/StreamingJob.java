package id.ac.ui.cs.netlog;

import java.util.Collection;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.cicflowmeter.optimized.FlowStats;
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

		Configuration config = new Configuration();
		config.setString("state.backend.rocksdb.memory.managed", "true");
		config.setString("state.backend.rocksdb.memory.fixed-per-slot", "256mb");
		config.setString("state.checkpoints.dir", "file:///tmp/flink-checkpoints");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
		EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
		backend.setRocksDBOptions(new RocksDBOptionsFactory() {
			@Override
			public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions
					.setIncreaseParallelism(4)
					.setUseFsync(false)
					.setMaxOpenFiles(-1)  // Unlimited open files
					.setMaxBackgroundJobs(4)
					.setAllowConcurrentMemtableWrite(true)
					.setEnableWriteThreadAdaptiveYield(false);
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions
					.setTableFormatConfig(
						new BlockBasedTableConfig()
							.setBlockSize(64 * 1024)    // 64KB
							.setBlockCacheSize(256 * 1024 * 1024)  // 256MB block cache
							.setCacheIndexAndFilterBlocks(true)
							.setPinL0FilterAndIndexBlocksInCache(true)
							.setBlockRestartInterval(16)
					)
					.setWriteBufferSize(64 * 1024 * 1024)  // 64MB write buffer
					.setMaxWriteBufferNumber(4)
					.setMinWriteBufferNumberToMerge(2)
					.setLevel0FileNumCompactionTrigger(4);
			}
		});
		env.setStateBackend(backend);
		env.enableCheckpointing(100000);

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
