package id.ac.ui.cs.netlog.utils;

import java.time.Instant;

public class TimeUtils {
    public static Long getCurrentTimeNano() {
        Instant instant = Instant.now();
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    public static Long getCurrentTimeMicro() {
        return getCurrentTimeNano() / 1000L;
    }
}
