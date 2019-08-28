package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;

public abstract class Classifier implements Transformer {

    protected String columnName;
    protected String outputColumnName = "_OUTLIER";
    protected DataFrame modelInfo;

    public Classifier(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public Classifier setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    public DataFrame getModelInfo(){
        return modelInfo;
    }

    public void setModelInfo(DataFrame modelInfo){
        this.modelInfo = modelInfo;
    }

    /**
     * @param outputColumnName Which column to write the classification results.
     * @return this
     */
    public Classifier setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
        return this;
    }
}
