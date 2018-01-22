package alexp.macrobase.ingest;

public class TableRange {
    private Integer startRow;
    private Integer startColumn;
    private Integer endRow;
    private Integer endColumn;

    public TableRange(Integer startRow, Integer startColumn, Integer endRow, Integer endColumn) {
        this.startRow = startRow;
        this.startColumn = startColumn;
        this.endRow = endRow;
        this.endColumn = endColumn;
    }

    public TableRange(Integer startRow, Integer startColumn) {
        this(startRow, startColumn, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public TableRange() {
        this(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public Integer getStartRow() {
        return startRow;
    }

    public Integer getStartColumn() {
        return startColumn;
    }

    public Integer getEndRow() {
        return endRow;
    }

    public Integer getEndColumn() {
        return endColumn;
    }
}
