package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UDP extends Transport {
    @JsonProperty("src_port")
    private Integer srcPort;

    @JsonProperty("dst_port")
    private Integer dstPort;

    @JsonProperty("payload_length")
    private Long payloadLength;

    @JsonProperty("header_length")
    private Long headerLength;
}
