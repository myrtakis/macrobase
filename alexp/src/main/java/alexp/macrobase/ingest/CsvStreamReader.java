package alexp.macrobase.ingest;

import alexp.macrobase.utils.LineBuffer;
import alexp.macrobase.utils.ThrowingConsumer;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvStreamReader implements StreamingDataFrameLoader {
    private final String filePath;
    private final List<String> requiredColumns;
    private Map<String, Schema.ColType> columnTypes = new HashMap<>();
    private int maxBatchSize = 5000;

    public CsvStreamReader(String filePath, List<String> requiredColumns) {
        this.filePath = filePath;
        this.requiredColumns = requiredColumns;
    }

    @Override
    public CsvStreamReader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    public CsvStreamReader setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    @Override
    public void load(ThrowingConsumer<DataFrame> resultCallback) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

        String header = in.readLine();

        LineBuffer inputBuff = new LineBuffer();
        resetLineBuffer(inputBuff, header);
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            if (inputBuff.count() > maxBatchSize) {
                resultCallback.accept(parseCsv(inputBuff.toString()));
                resetLineBuffer(inputBuff, header);
            }

            inputBuff.appendLine(inputLine);
        }
        in.close();

        if (inputBuff.count() > 0) {
            resultCallback.accept(parseCsv(inputBuff.toString()));
        }
    }

    private void resetLineBuffer(LineBuffer lineBuffer, String header) {
        lineBuffer.clear();
        lineBuffer.appendLine(header);
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
