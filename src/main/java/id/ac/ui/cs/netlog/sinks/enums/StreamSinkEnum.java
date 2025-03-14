package id.ac.ui.cs.netlog.sinks.enums;

public enum StreamSinkEnum {
    PRINT("print"),
    KAFKA("kafka");

    private final String value;

    StreamSinkEnum(String value) {
        this.value = value;
    }

    public static StreamSinkEnum fromString(String text) {
        if (text == null) {
            throw new IllegalArgumentException("String value cannot be null");
        }

        for (StreamSinkEnum status : StreamSinkEnum.values()) {
            if (status.value.equalsIgnoreCase(text)) {
                return status;
            }
        }

        throw new IllegalArgumentException(
            String.format("No constant with text '%s' found", text)
        );
    }

    @Override
    public String toString() {
        return this.value;
    }
}
