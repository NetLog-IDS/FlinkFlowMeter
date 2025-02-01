package id.ac.ui.cs.netlog.utils;

import lombok.Data;

@Data
public class MutableInt {
    private int value = 0; // note that we start at 1 since we're counting

	public void increment() {
        ++value;
    }
}
