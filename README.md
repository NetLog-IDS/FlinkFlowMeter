# FlinkFlowMeter

[GintEngelen's Fixed CICFlowMeter](https://github.com/GintsEngelen/CICFlowMeter) Integration with Apache Flink.

## Files
- `pom.xml`: Project configuration and dependencies
- `src/main/java/id/ac/ui/cs/netlog`
    - `data`: Contains models for preprocessing such as `Flow`, `FlowStats`, `Packet`, etc.
    - `modes`: Contains configuration for FlinkFlowMeter modes. Currently, there's only `ordered` mode for ordered messages.
    - `operators`: Contains operators for the processing such as `ExtractPacketInfo`, `FlowGenerator`, and `ExtractFlowStats`.
    - `serialization`: Serialization/deserialization classes for/from Kafka.
    - `sinks`: Configuration for sinks. Currently, Kafka and print (for debugging) sinks are supported.
    - `source`: Configuration for sources. Currently, Kafka and socket stream (for debugging) sources are supported.
    - `utils`: Helper class/functions.
    - `StreamingJob.java`: Job declaration class.

## Running
- First, run Apache Flink using commands on [infrastructure repository](https://github.com/NetLog-IDS/infrastructure).
- After running, go to the UI at `http://<jobmanager-ip>:<jobmanager-port>` (usually port 8082).
- Submit the `.jar` file of FlinkFlowMeter.
- Run the job with configuration below:

    ```
    --source kafka --source-servers <kafka-ip>:<kafka-port> --source-topic network-traffic --source-group <group-name> --watermark-strategy monotonous --sink kafka --sink-servers <kafka-ip>:<kafka-port> --sink-topic network-flows --mode ordered
    ```
