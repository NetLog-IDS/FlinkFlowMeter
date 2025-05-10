package id.ac.ui.cs.netlog.operators;

import org.apache.flink.api.common.functions.MapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.packets.Packet;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PacketParser implements MapFunction<String, Packet> {
    private final ObjectMapper objectMapper;

    @Override
    public Packet map(String jsonString) {
        try {
            return this.objectMapper.readValue(jsonString, Packet.class);
        } catch (Exception exc) {
            throw new IllegalArgumentException("Invalid json");
        }
    }
}
