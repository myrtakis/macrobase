package alexp.macrobase.ingest;

public class Uri {
    public enum Type {
        UNKNOWN,
        CSV,
        XLSX,
        HTTP,
        JDBC
    }

    private final String originalString;

    private final String path;
    private final String typeString;
    private final Type type;

    public Uri(String originalString) {
        this.originalString = originalString;

        if (!originalString.contains(":")) {
            type = Type.UNKNOWN;
            path = originalString;
            typeString = "";
        } else {
            typeString = originalString.substring(0, originalString.indexOf(":")).toLowerCase();

            switch (typeString.toLowerCase()) {
                case "csv":
                    type = Type.CSV;
                    path = originalString.substring(typeString.length() + 1).replace("//", "");
                    break;
                case "xls":
                    type = Type.XLSX;
                    path = originalString.substring(typeString.length() + 1).replace("//", "");
                    break;
                case "http":
                case "https":
                    type = Type.HTTP;
                    path = originalString;
                    break;
                case "jdbc":
                    type = Type.JDBC;
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
