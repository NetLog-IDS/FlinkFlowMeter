package id.ac.ui.cs.netlog.serialization.deserializers;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;

import id.ac.ui.cs.netlog.data.packets.Packet;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PacketDeserializationSchema implements DeserializationSchema<Packet> {
    private final ObjectMapper objectMapper;

    @Override
    public Packet deserialize(byte[] message) throws IOException {
        try {
            return this.objectMapper.readValue(message, Packet.class);
        } catch (Exception exc) {
            throw new IllegalArgumentException("Invalid json");
        }
    }

    @Override
    public TypeInformation<Packet> getProducedType() {
        return TypeInformation.of(Packet.class);
    }

    @Override
    public boolean isEndOfStream(Packet nextElement) {
        return false;
    }
}
