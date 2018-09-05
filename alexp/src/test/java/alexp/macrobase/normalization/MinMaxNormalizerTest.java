package alexp.macrobase.normalization;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import static org.junit.Assert.*;

public class MinMaxNormalizerTest {

    @Test
    public void minMaxNormalizerTest() throws Exception {
        DataFrame df = new DataFrame();
        df.addColumn("col", new double[] { 1.1, 2.3, 8.6, 0.4, 80.9, 128.0, 256.7, 341.8, 20.6 });

        Normalizer normalizer = new MinMaxNormalizer().setColumnName("col").setOutputColumnName("normalized");
        normalizer.process(df);
        DataFrame result = normalizer.getResults();

        assertEquals(2, result.getDoubleCols().size());
        assertArrayEquals(new double[] { 0.002, 0.006, 0.024, 0.000, 0.236, 0.374, 0.751, 1.000, 0.059 }, result.getDoubleColumnByName("normalized"), 0.001);
    }
}