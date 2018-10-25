package alexp.macrobase.outlier;

import alexp.macrobase.normalization.MinMaxNormalizer;
import alexp.macrobase.normalization.Normalizer;
import alexp.macrobase.utils.MathUtils;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.Arrays;
import java.util.OptionalDouble;

public class MAD extends Classifier {
    private double median;
    private double MAD;

    private DataFrame output;

    private int trainSize = 10000;
    private OptionalDouble threshold = OptionalDouble.empty(); // outputs score if not set

    private Normalizer normalizer;

    public MAD(String columnName) {
        super(columnName);
    }

    private void train(DataFrame input) {
        double[] metricColumn = input.getDoubleColumnByName(columnName);

        Arrays.sort(metricColumn);

        median = MathUtils.middle(metricColumn);

        double[] residuals = new double[metricColumn.length];
        for (int i = 0; i < metricColumn.length; i++) {
            residuals[i] = Math.abs(metricColumn[i] - median);
        }

        Arrays.sort(residuals);

        MAD =  MathUtils.middle(residuals);

        if (MAD == 0) {
            double trimmedMeanFallback = 0.05;
            int lowerTrimmedMeanIndex = (int) (residuals.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (residuals.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += residuals[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }

    @Override
    public void process(DataFrame input) throws Exception {
        train(input.limit(Math.min(trainSize, input.getNumRows() - 1))); // must be deep copy

        double[] metricColumn = input.getDoubleColumnByName(columnName);

        output = input.copy();

        double[] resultColumn = new double[metricColumn.length];
        for (int i = 0; i < metricColumn.length; i++) {
            resultColumn[i] = Math.abs(metricColumn[i] - median) / (MAD);
        }

        output.addColumn(outputColumnName, resultColumn);

        if (normalizer != null) {
            normalizer.setColumnName(outputColumnName).setOutputColumnName(outputColumnName);
            normalizer.process(output);
            output = normalizer.getResults();
        }

        if (threshold.isPresent()) {
            double[] v = output.getDoubleColumnByName(outputColumnName);

            for (int i = 0; i < metricColumn.length; i++) {
                v[i] = v[i] > threshold.getAsDouble() ? 1.0 : 0.0;
            }
        }
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    public int getTrainSize() {
        return trainSize;
    }

    public void setTrainSize(int trainSize) {
        this.trainSize = trainSize;
    }

    public void setThreshold(double threshold) {
        this.threshold = OptionalDouble.of(threshold);
    }

    public void setNormalizer(Normalizer normalizer) {
        this.normalizer = normalizer;
    }
}
