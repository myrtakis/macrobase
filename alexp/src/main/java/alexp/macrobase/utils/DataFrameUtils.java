package alexp.macrobase.utils;

import com.github.chen0040.data.frame.BasicDataFrame;
import com.github.chen0040.data.frame.DataRow;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;

public class DataFrameUtils {
    public static ArrayList<RealVector> toRowsRealVector(DataFrame data, String[] columns) {
        ArrayList<double[]> colsValues = data.getDoubleColsByName(Arrays.asList(columns));
        ArrayList<RealVector> result = new ArrayList<>(data.getNumRows());
        for (int i = 0; i < data.getNumRows(); i++) {
            RealVector row = new ArrayRealVector(columns.length);
            for (int j = 0; j < colsValues.size(); j++) {
                row.setEntry(j, colsValues.get(j)[i]);
            }
            result.add(row);
        }
        return result;
    }

    public static com.github.chen0040.data.frame.DataFrame toRowDataFrame(DataFrame data, String[] columns) {
        ArrayList<double[]> colsValues = data.getDoubleColsByName(Arrays.asList(columns));
        BasicDataFrame result = new BasicDataFrame();
        for (int i = 0; i < data.getNumRows(); i++) {
            DataRow row = result.newRow();
            for (int j = 0; j < colsValues.size(); j++) {
                row.setCell(columns[j], colsValues.get(j)[i]);
            }
            result.addRow(row);
        }
        return result;
    }

    public static List<double[]> toRowArray(DataFrame data, String[] columns) {
        ArrayList<double[]> colsValues = data.getDoubleColsByName(Arrays.asList(columns));
        ArrayList<double[]> result = new ArrayList<>(data.getNumRows());
        for (int i = 0; i < data.getNumRows(); i++) {
            double[] row = new double[colsValues.size()];
            for (int j = 0; j < colsValues.size(); j++) {
                row[j] = colsValues.get(j)[i];
            }
            result.add(row);
        }
        return result;
    }

    public static void saveToCsv(String filePath, DataFrame data) throws IOException {
        Files.createDirectories(Paths.get(filePath).getParent());

        CSVDataFrameWriter writer = new CSVDataFrameWriter();
        writer.writeToStream(data, new FileWriter(new File(filePath)));
    }

    public static DataFrame filterByAll(DataFrame data, Map<String, String> attributes) {
        if (attributes.isEmpty()) {
            return data;
        }

        String column = attributes.keySet().iterator().next();
        String val = attributes.get(column);

        Map<String, String> rest = new HashMap<>(attributes);
        rest.remove(column);

        return filterByAll(data.filter(column, (Predicate<Object>) val::equals), rest);
    }

    public static List<double[]> getDoubleRows(DataFrame data, String... columns) {
        List<double[]> rows = new ArrayList<>();
        List<double[]> columnsValues = new ArrayList<>();
        for (String column : columns) {
            columnsValues.add(data.getDoubleColumnByName(column));
        }

        for (int i = 0; i < columnsValues.get(0).length; i++) {
            double[] row = new double[columnsValues.size()];
            for (int j = 0; j < columnsValues.size(); j++) {
                row[j] = columnsValues.get(j)[i];
            }
            rows.add(row);
        }
        return rows;
    }
}
