package id.ac.ui.cs.netlog.data.packets;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TCP.class, name = "tcp"),
    @JsonSubTypes.Type(value = UDP.class, name = "udp")
})
@Data
public class Transport {
    private String type;
}
