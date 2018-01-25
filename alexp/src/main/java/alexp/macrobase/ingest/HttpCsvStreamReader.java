package alexp.macrobase.ingest;

import alexp.macrobase.utils.ThrowingConsumer;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.function.Consumer;

public class HttpCsvStreamReader implements StreamingDataFrameLoader {
    private final String url;
    private final List<String> requiredColumns;
    private Map<String, Schema.ColType> columnTypes = new HashMap<>();

    public HttpCsvStreamReader(String url, List<String> requiredColumns) throws IOException {
        this.url = url;
        this.requiredColumns = requiredColumns;
    }

    @Override
    public HttpCsvStreamReader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public void load(ThrowingConsumer<DataFrame> resultCallback) throws Exception {
        URL url = new URL(this.url);
        URLConnection conn = url.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));

        String inputLine;
        StringBuilder inputBuff = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            if (inputLine.trim().equalsIgnoreCase("_END_PART_")) {
                resultCallback.accept(parseCsv(inputBuff.toString()));
                inputBuff = new StringBuilder();
            } else {
                inputBuff.append(inputLine).append("\n");
            }
        }
        in.close();

        if (inputBuff.length() > 0) {
            resultCallback.accept(parseCsv(inputBuff.toString()));
        }
    }

    private DataFrame parseCsv(String csvData) throws Exception {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser csvParser = new CsvParser(settings);
        csvParser.beginParsing(new ByteArrayInputStream(csvData.getBytes("UTF-8")));

        DataFrameLoader loader = new CSVDataFrameParser(csvParser, requiredColumns);
        loader.setColumnTypes(columnTypes);

        return loader.load();
    }
}
