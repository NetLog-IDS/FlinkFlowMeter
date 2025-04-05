package id.ac.ui.cs.netlog.data.packets;

// import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Layers {
    // private Frame frame;

    // @JsonProperty("data_link")
    // private DataLink ethernet;

    private Network network;

    private Transport transport;
}
