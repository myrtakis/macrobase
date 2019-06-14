package alexp.macrobase.explanation;

import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public abstract class Explanation implements Transformer {

    protected   String[]            columns;
    protected   AlgorithmConfig     classifierConf;
    protected   String              outputColumnName        = "_OUTLIER";
    protected   String              relSubspaceColumnName   = "__REL_SUBSPACES";
    private     ExplanationSettings explanationSettings;
    private     HashSet<Integer>    pointsToExplain;


    public Explanation(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) {
        this.columns = columns;
        this.classifierConf = classifierConf;
        this.explanationSettings = explanationSettings;
    }

    /**
     * This function must be implemented from each explanation algorithm.
     */
    public abstract <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces);

    public HashSet<Integer> getPointsToExplain() {
        if(explanationSettings.dictatedOutlierMethod()) {
            return pointsToExplain == null ? new HashSet<>(explanationSettings.getDictatedOutliers()) : pointsToExplain;
        }
        // TODO else do the detection and return the outlier points to explain
        return null;
    }

    public int getDatasetDimensionality() {
        return columns.length;
    }

    public String subspaceToString(Collection<Integer> subspaceFeatures, double score) {
        return "[" + Joiner.on(" ").join(subspaceFeatures) + "] " + score + ";";
    }

    public String[] getColumns() {
        return columns;
    }

    public AlgorithmConfig getClassifierConf() {
        return classifierConf;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }
}
