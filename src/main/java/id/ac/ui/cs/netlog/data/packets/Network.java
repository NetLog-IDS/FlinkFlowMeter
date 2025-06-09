package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Network {
    @JsonProperty("version")
    private Long version;

    @JsonProperty("src")
    private String src;

    @JsonProperty("dst")
    private String dst;
}
