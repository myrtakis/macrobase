package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.Arrays;

public class MAD extends Classifier {
    private double median;
    private double MAD;

    private DataFrame output;

    private int trainSize = 10000;

    public MAD(String columnName) {
        super(columnName);
    }

    private void train(DataFrame input) {
        double[] metricColumn = input.getDoubleColumnByName(columnName);

        Arrays.sort(metricColumn);

        if (metricColumn.length % 2 == 0) {
            median = (metricColumn[metricColumn.length / 2 - 1] + metricColumn[metricColumn.length / 2]) / 2;
        } else {
            median = metricColumn[(int) Math.ceil(metricColumn.length / 2)];
        }

        double[] residuals = new double[metricColumn.length];
        for (int i = 0; i < metricColumn.length; i++) {
            residuals[i] = Math.abs(metricColumn[i] - median);
        }

        Arrays.sort(residuals);

        if (metricColumn.length % 2 == 0) {
            MAD = (residuals[metricColumn.length / 2 - 1] +
                   residuals[metricColumn.length / 2]) / 2;
        } else {
            MAD = residuals[(int) Math.ceil(metricColumn.length / 2)];
        }

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
    public void process(DataFrame input) {
        train(input.limit(trainSize));

        double[] metricColumn = input.getDoubleColumnByName(columnName);

        output = input.copy();

        double[] resultColumn = new double[metricColumn.length];
        for (int i = 0; i < metricColumn.length; i++) {
            resultColumn[i] = Math.abs(metricColumn[i] - median) / (MAD);
        }

        output.addColumn(outputColumnName, resultColumn);
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
}
