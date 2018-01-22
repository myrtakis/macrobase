package alexp.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.*;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.*;
import java.io.*;
import java.util.*;

public class XlsxDataFrameParser implements DataFrameLoader {
    private class ColumnInfo {
        public final String name;
        public final Schema.ColType type;

        public ColumnInfo(String name, Schema.ColType type) {
            this.name = name;
            this.type = type;
        }
    }

    private Sheet sheet;
    private final HashSet<String> requiredColumns;
    private final TableRange tableRange;
    private Map<String, Schema.ColType> columnTypes = new HashMap<>();

    public XlsxDataFrameParser(String filePath, Collection<String> requiredColumns, int sheetIndex, TableRange tableRange) throws IOException {
        this.requiredColumns = new HashSet<>(requiredColumns);
        this.tableRange = tableRange;

        Workbook wb = new XSSFWorkbook(filePath);
        sheet = wb.getSheetAt(sheetIndex);
    }

    public XlsxDataFrameParser(String filePath, List<String> requiredColumns, int sheetIndex) throws IOException {
        this(filePath, requiredColumns, sheetIndex, new TableRange());
    }

    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        final int firstRowIndex = tableRange.getStartRow();
        final int firstColumnIndex = tableRange.getStartColumn();
        final int lastRowIndex = Math.min(tableRange.getEndRow(), sheet.getLastRowNum());
        final int lastColumnIndex = Math.min(tableRange.getEndColumn(), sheet.getRow(firstRowIndex).getLastCellNum() - 1);

        Map<Integer, ColumnInfo> columnIndexInfoMap = new HashMap<>();
        List<Integer> requiredColumnsIndexes = new ArrayList<>();

        Row headerRow = sheet.getRow(firstRowIndex);
        Row firstDataRow = sheet.getRow(firstRowIndex + 1);
        for (int i = firstColumnIndex; i <= lastColumnIndex; i++) {
            String name = headerRow.getCell(i).getStringCellValue();
            if (requiredColumns.contains(name)) {
                Schema.ColType colType = columnTypes.getOrDefault(name, sheetCellTypeToSchemaColType(firstDataRow.getCell(i).getCellTypeEnum()));
                columnIndexInfoMap.put(i, new ColumnInfo(name, colType));
                requiredColumnsIndexes.add(i);
            }
        }

        Schema schema = new Schema();
        for (Integer ind : requiredColumnsIndexes) {
            ColumnInfo columnInfo = columnIndexInfoMap.get(ind);
            schema.addColumn(columnInfo.type, columnInfo.name);
        }

        // should use faster constructor accepting string and double arrays instead of Row
        List<edu.stanford.futuredata.macrobase.datamodel.Row> rows = new ArrayList<>();
        for (int i = firstRowIndex + 1; i <= lastRowIndex; i++) {
            Row row = sheet.getRow(i);
            List<Object> vals = new ArrayList<>();
            for (Integer cellInd : requiredColumnsIndexes) {
                ColumnInfo columnInfo = columnIndexInfoMap.get(cellInd);
                Cell cell = row.getCell(cellInd);
                Object val;
                switch (columnInfo.type) {
                    case STRING:
                        val = cell.getStringCellValue();
                        break;
                    case DOUBLE:
                        val = cell.getNumericCellValue();
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported col type " + columnInfo.type);
                }
                vals.add(val);
            }
            rows.add(new edu.stanford.futuredata.macrobase.datamodel.Row(schema, vals));
        }

        return new DataFrame(schema, rows);
    }

    private Schema.ColType sheetCellTypeToSchemaColType(CellType cellType) {
        switch (cellType) {
            case NUMERIC:
                return Schema.ColType.DOUBLE;
            case STRING:
                return Schema.ColType.STRING;
            default:
                throw new IllegalArgumentException("Unsupported col type " + cellType);
        }
    }
}
