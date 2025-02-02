package id.ac.ui.cs.netlog.source.enums;

public enum StreamSourceEnum {
    KAFKA("kafka"),
    SOCKET("socket");

    private final String value;

    StreamSourceEnum(String value) {
        this.value = value;
    }

    public static StreamSourceEnum fromString(String text) {
        if (text == null) {
            throw new IllegalArgumentException("String value cannot be null");
        }

        for (StreamSourceEnum status : StreamSourceEnum.values()) {
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
