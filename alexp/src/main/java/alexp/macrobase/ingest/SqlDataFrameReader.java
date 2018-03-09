package alexp.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlDataFrameReader implements DataFrameLoader {
    private final List<String> requiredColumns;
    private final String query;
    private Map<String, Schema.ColType> columnTypes = new HashMap<>();

    private Connection connection;
    private Statement statement;

    private Schema schema = null;

    public SqlDataFrameReader(String url, List<String> requiredColumns, String query) throws IOException, SQLException {
        this.requiredColumns = requiredColumns;
        this.query = query;

        connection = DriverManager.getConnection(url);
        statement = connection.createStatement();
    }

    @Override
    public SqlDataFrameReader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        String query = this.query;

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
            } while (rs.next());

            return new DataFrame(schema.copy(), rows);
        }

        return null;
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
