package gmn.macrobase.explanation;

import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import gmn.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;

import java.util.List;

public abstract class Explanation implements Transformer {

    protected   String[]            columns;
    protected   AlgorithmConfig     classifierConf;
    protected   String              outputColumnName = "_RELEVANT_SUBSPACES";
    private     List<Double>        outlierPoints; // The ids (row number) of the outlier points. Those points can be detected by an anomaly detection algorithm or detected by another external source or the dataset's ground truth
    private     ExplanationSettings explanationSettings;


    public Explanation(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) {
        this.columns = columns;
        this.classifierConf = classifierConf;
        this.explanationSettings = explanationSettings;
    }

    public List<Double> getPointsToExplain() {
        if(explanationSettings.dictatedOutlierMethod())
            return explanationSettings.getDictatedOutliers();
        // TODO else do the detection and return the outlier points to explain
        return null;
    }

    public String[] getColumns() {
        return columns;
    }

    public AlgorithmConfig getClassifierConf() {
        return classifierConf;
    }
}
