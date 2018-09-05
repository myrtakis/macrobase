package alexp.macrobase.normalization;

import edu.stanford.futuredata.macrobase.operator.Transformer;

public abstract class Normalizer implements Transformer {
    protected String columnName;
    protected String outputColumnName;

    public String getColumnName() {
        return columnName;
    }

    public Normalizer setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    public Normalizer setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
        return this;
    }
}
