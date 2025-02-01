package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Packet {
    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("layers")
    private Layers layers;
}
