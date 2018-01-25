package alexp.macrobase.ingest;

public class Uri {
    public enum Type {
        UNKNOWN,
        CSV,
        XLSX,
        HTTP
    }

    private final String originalString;

    private final String path;
    private final String typeString;
    private final Type type;

    public Uri(String originalString) {
        this.originalString = originalString;

        if (!originalString.contains("://")) {
            type = Type.UNKNOWN;
            path = originalString;
            typeString = "";
        } else {
            String[] parts = originalString.split("://");

            typeString = parts[0].toLowerCase();

            switch (parts[0].toLowerCase()) {
                case "csv":
                    type = Type.CSV;
                    path = parts[1];
                    break;
                case "xls":
                    type = Type.XLSX;
                    path = parts[1];
                    break;
                case "http":
                case "https":
                    type = Type.HTTP;
                    path = originalString;
                    break;
                default:
                    type = Type.UNKNOWN;
                    path = originalString;
                    break;
            }
        }
    }

    public String getOriginalString() {
        return originalString;
    }

    public String getPath() {
        return path;
    }

    public String getTypeString() {
        return typeString;
    }

    public Type getType() {
        return type;
    }
}
