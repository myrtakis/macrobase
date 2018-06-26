package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;

public abstract class MultiMetricClassifier extends Classifier {
    protected String[] columns;

    public MultiMetricClassifier(String columnName) {
        super(columnName);
        this.columns = new String[] { columnName };
    }

    public MultiMetricClassifier(String[] columns) {
        super(columns.length == 1 ? columns[0] : null);
        this.columns = columns;
    }

    public String[] getColumns() {
        return columns;
    }

    @Override
    public Classifier setColumnName(String columnName) {
        this.columns = new String[] { columnName };
        return super.setColumnName(columnName);
    }
}
