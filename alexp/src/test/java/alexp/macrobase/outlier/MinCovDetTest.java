package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class MinCovDetTest {
    private static double getMahalanobisApache(RealVector mean,
                                               RealMatrix inverseCov,
                                               RealVector vec) {
        // sqrt((vec-mean)^T S^-1 (vec-mean))
        RealMatrix vecT = new Array2DRowRealMatrix(vec.toArray());
        RealMatrix meanT = new Array2DRowRealMatrix(mean.toArray());
        RealMatrix vecSubtractMean = vecT.subtract(meanT);

        return Math.sqrt(vecSubtractMean.transpose()
                .multiply(inverseCov)
                .multiply(vecSubtractMean).getEntry(0, 0));
    }

    @Test
    public void testMahalanobis() {
        int dim = 100;
        int nsamples = 1000;
        Random r = new Random(0);

        List<ArrayRealVector> testData = new ArrayList<>();
        for (int i = 0; i < nsamples; ++i) {
            double[] sample = new double[dim];
            for (int d = 0; d < dim; ++d) {
                sample[d] = d % 2 == 0 ? r.nextDouble() : r.nextGaussian();
            }
            testData.add(new ArrayRealVector(sample));
        }
        Schema schema = new Schema();
        List<Row> rows = testData.stream().map(v -> new Row(schema, Arrays.stream(v.getDataRef()).boxed().collect(Collectors.toList()))).collect(Collectors.toList());
        List<String> columns = IntStream.range(0, dim).mapToObj(n -> "col" + n).collect(Collectors.toList());
        columns.forEach(c -> schema.addColumn(Schema.ColType.DOUBLE, c));
        DataFrame testDataFrame = new DataFrame(schema, rows);

        MinCovDet trainer = new MinCovDet(columns.toArray(new String[0]));
        trainer.setAlpha(0.5);
        trainer.setStoppingDelta(0.0001);
        trainer.setRandom(r);

        trainer.train(testDataFrame);

        RealMatrix inverseCov = trainer.getInverseCovariance();
        RealVector mean = trainer.getMean();

        for (RealVector d : testData) {
            assertEquals(trainer.score(d), getMahalanobisApache(mean, inverseCov, new ArrayRealVector(d)), 0.01);
        }
    }

}