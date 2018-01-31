package alexp.macrobase.ingest;

import alexp.macrobase.utils.ThrowingConsumer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlStreamReader implements StreamingDataFrameLoader {
    private final String url;
    private final List<String> requiredColumns;
    private final String query;
    private final String idColumn;
    private Map<String, Schema.ColType> columnTypes = new HashMap<>();

    private Connection connection;
    private Statement statement;

    private Schema schema = null;

    private Integer maxId = -1;

    public SqlStreamReader(String url, List<String> requiredColumns, String query, String idColumn) throws IOException, SQLException {
        this.url = url;
        this.requiredColumns = requiredColumns;
        this.query = query;
        this.idColumn = idColumn;

        connection = DriverManager.getConnection(url);
        statement = connection.createStatement();
    }

    @Override
    public SqlStreamReader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public void load(ThrowingConsumer<DataFrame> resultCallback) throws Exception {
        while (true) {
            DataFrame df = loadData();

            if (df != null) {
                resultCallback.accept(df);
            }

            Thread.sleep(1000);
        }
    }

    private DataFrame loadData() throws Exception {
        String query = String.format("%s WHERE %s > %d ORDER BY %s", this.query, idColumn, maxId, idColumn);

        ResultSet rs = statement.executeQuery(query);
        if (rs.next()) {
            if (schema == null) {
                schema = createSchema(rs);
            }

            // should use faster constructor accepting string and double arrays instead of Row
            List<Row> rows = new ArrayList<>();

            do {
                List<Object> vals = new ArrayList<>();
                for (String colName : requiredColumns) {
                    switch (schema.getColumnTypeByName(colName)) {
                        case STRING:
                            vals.add(rs.getString(colName));
                            break;
                        case DOUBLE:
                            vals.add(rs.getDouble(colName));
                            break;
                    }
                }
                rows.add(new Row(schema, vals));

                maxId = rs.getInt(idColumn);
            } while (rs.next());

            return new DataFrame(schema.copy(), rows);
        } else {
            return null;
        }
    }

    private Schema createSchema(ResultSet rs) throws Exception {
        ResultSetMetaData metadata = rs.getMetaData();

        Schema schema = new Schema();

        for (String colName : requiredColumns) { // must to preserve order
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                String name = metadata.getColumnName(i);
                if (name.contains(colName)) {
                    schema.addColumn(colTypeToSchemaColType(metadata.getColumnType(i)), name);
                    break;
                }
            }
        }

        return schema;
    }

    private Schema.ColType colTypeToSchemaColType(Integer colType) {
        switch (colType) {
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.BIGINT:
                return Schema.ColType.DOUBLE;
            case Types.VARCHAR:
                return Schema.ColType.STRING;
            default:
                throw new IllegalArgumentException("Unsupported col type " + colType);
        }
    }
}
