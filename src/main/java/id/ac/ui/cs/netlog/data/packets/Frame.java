package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Frame {
    @JsonProperty("time")
    private String time;

    @JsonProperty("number")
    private Long number;

    @JsonProperty("length")
    private Long length;

    @JsonProperty("protocols")
    private String protocols;
}
