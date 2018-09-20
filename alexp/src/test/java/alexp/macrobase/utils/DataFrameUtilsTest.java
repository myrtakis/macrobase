package alexp.macrobase.utils;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DataFrameUtilsTest {

    @Test
    public void getDoubleRowsTest() {
        DataFrame df = new DataFrame();
        df.addColumn("c1", new double[] { 1.0, 2.0, 3.0});
        df.addColumn("c2", new double[] { 4.0, 5.0, 6.0});
        df.addColumn("extra", new double[] { 999.0, 999.0, 999.0});

        List<double[]> rows = DataFrameUtils.getDoubleRows(df, "c1", "c2");

        assertEquals(3, rows.size());
        assertArrayEquals(new double[] { 1.0, 4.0 }, rows.get(0), 0.001);
        assertArrayEquals(new double[] { 2.0, 5.0 }, rows.get(1), 0.001);
        assertArrayEquals(new double[] { 3.0, 6.0 }, rows.get(2), 0.001);
    }
}