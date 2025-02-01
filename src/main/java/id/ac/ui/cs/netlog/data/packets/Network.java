package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Network {
    @JsonProperty("version")
    private Long version;

    @JsonProperty("hdr_len")
    private Long hdrLen;

    @JsonProperty("tos")
    private Long tos;

    @JsonProperty("len")
    private Long len;

    @JsonProperty("id")
    private Long id;

    @JsonProperty("flags")
    private Long flags;

    @JsonProperty("flags_rb")
    private Long flagsRb;

    @JsonProperty("flags_df")
    private Long flagsDf;

    @JsonProperty("flags_mf")
    private Long flagsMf;

    @JsonProperty("frag_offset")
    private Long fragOffset;

    @JsonProperty("ttl")
    private Long ttl;

    @JsonProperty("proto")
    private Long proto;

    @JsonProperty("checksum")
    private Long checksum;

    @JsonProperty("src")
    private String src;

    @JsonProperty("dst")
    private String dst;
}
