package alexp.macrobase.utils;

import com.github.chen0040.data.frame.BasicDataFrame;
import com.github.chen0040.data.frame.DataRow;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.ArrayList;
import java.util.Arrays;

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
}
