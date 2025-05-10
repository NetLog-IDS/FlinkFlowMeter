package id.ac.ui.cs.netlog.data.packets;

import lombok.Data;

@Data
public class Layers {
    private Network network;

    private Transport transport;
}
