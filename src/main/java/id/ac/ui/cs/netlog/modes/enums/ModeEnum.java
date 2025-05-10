package id.ac.ui.cs.netlog.modes.enums;

public enum ModeEnum {
    ORDERED("ordered");
    
    private final String value;

    ModeEnum(String value) {
        this.value = value;
    }

    public static ModeEnum fromString(String text) {
        if (text == null) {
            throw new IllegalArgumentException("String value cannot be null");
        }

        for (ModeEnum status : ModeEnum.values()) {
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
