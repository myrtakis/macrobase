package alexp.macrobase.utils;

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
}
