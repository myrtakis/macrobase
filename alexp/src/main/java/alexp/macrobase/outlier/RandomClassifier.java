package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.Random;

public class RandomClassifier extends MultiMetricClassifier {
    private boolean binary = true;

    private DataFrame output;

    public RandomClassifier(String columnName) {
        super(columnName);
    }

    public RandomClassifier(String[] columns) {
        super(columns);
    }

    public RandomClassifier() {
        super("RANDOM_CLASSIFIER");
    }

    @Override
    public void process(DataFrame input) throws Exception {
        Random rand = new Random();

        output = input.copy();

        double[] resultColumn = new double[input.getNumRows()];
        for (int i = 0; i < resultColumn.length; i++) {
            if (binary) {
                resultColumn[i] = rand.nextBoolean() ? 1.0 : 0.0;
            } else {
                resultColumn[i] = rand.nextDouble();
            }
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    public boolean isBinary() {
        return binary;
    }

    public void setBinary(boolean binary) {
        this.binary = binary;
    }
}
