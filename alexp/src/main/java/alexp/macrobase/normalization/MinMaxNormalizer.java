package alexp.macrobase.normalization;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.Arrays;

public class MinMaxNormalizer extends Normalizer {

    private DataFrame output;

    @Override
    public void process(DataFrame input) throws Exception {
        double[] values = input.getDoubleColumnByName(columnName);

        double min = Arrays.stream(values).min().getAsDouble();
        double max = Arrays.stream(values).max().getAsDouble();

        double[] outputValues = Arrays.stream(values).map(v -> (v - min) / (max - min)).toArray();

        output = input.copy();
        output.addColumn(outputColumnName, outputValues);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

}
