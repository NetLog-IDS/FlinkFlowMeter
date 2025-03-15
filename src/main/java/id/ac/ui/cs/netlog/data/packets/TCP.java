package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TCP extends Transport {
    @JsonProperty("src_port")
    private Integer srcPort;

    @JsonProperty("dst_port")
    private Integer dstPort;

    @JsonProperty("flags")
    private Integer flags;

    @JsonProperty("window")
    private Integer window;

    @JsonProperty("payload_length")
    private Long payloadLength;

    @JsonProperty("header_length")
    private Long headerLength;
}
