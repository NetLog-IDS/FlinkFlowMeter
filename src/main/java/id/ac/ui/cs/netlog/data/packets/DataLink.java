package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class DataLink {
    @JsonProperty("dst")
    private String dst;

    @JsonProperty("src")
    private String src;

    @JsonProperty("type")
    private Long type;

    @JsonProperty("header_size")
    private Long headerSize;

    @JsonProperty("trailer_size")
    private Long trailerSize;
}
