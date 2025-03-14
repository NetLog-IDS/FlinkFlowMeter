package id.ac.ui.cs.netlog.modes.factory;

import id.ac.ui.cs.netlog.modes.OrderedMode;
import id.ac.ui.cs.netlog.modes.StreamMode;
import id.ac.ui.cs.netlog.modes.UnorderedMode;
import id.ac.ui.cs.netlog.modes.enums.ModeEnum;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ModeFactory {
    public StreamMode getMode(ModeEnum mode) {
        if (mode.equals(ModeEnum.ORDERED)) {
            return new OrderedMode();
        } else {
            return new UnorderedMode();
        }
    }
}
