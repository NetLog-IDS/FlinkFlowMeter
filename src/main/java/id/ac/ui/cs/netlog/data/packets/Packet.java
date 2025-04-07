package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Packet {
    @JsonProperty("id")
    private String id;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("order")
    private Long order;

    @JsonProperty("publisher_id")
    private String publisherId;

    @JsonProperty("layers")
    private Layers layers;

    @JsonProperty("sniff_time")
    private Long sniffTime;
}
