package alexp.macrobase.utils;

public class LineBuffer {
    private final StringBuilder sb = new StringBuilder();
    private int count = 0;
    private String delimiter = "\n";

    public int count() {
        return count;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String toString() {
        return sb.toString();
    }

    public void appendLine(String line) {
        sb.append(line).append(delimiter);
        count++;
    }

    public void clear() {
        sb.setLength(0);
        count = 0;
    }
}
