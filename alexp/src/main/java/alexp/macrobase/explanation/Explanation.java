package alexp.macrobase.explanation;

import alexp.macrobase.explanation.hics.HiCS;
import alexp.macrobase.explanation.lookOut.LookOut;
import alexp.macrobase.explanation.utils.Subspace;
import alexp.macrobase.explanation.utils.anomalyDetectorsWrapper.OutlierDetectorsWrapper;
import alexp.macrobase.outlier.mcod.Data;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import javafx.util.Pair;

import javax.naming.ldap.PagedResultsControl;
import java.util.*;

public abstract class Explanation implements Transformer {

    protected   String[]            columns;
    protected   AlgorithmConfig     classifierConf;
    protected   String              datasetPath;
    protected   String              outputColumnName        = "_OUTLIER";
    protected   String              relSubspaceColumnName   = "__REL_SUBSPACES";
    private     ExplanationSettings explanationSettings;
    private     HashSet<Integer>    pointsToExplain;
    private     boolean             invokePythonClassifier;

    private final String subspaceDelimiter = " .,-\t\n[]{}";

    public Explanation(String[] columns, AlgorithmConfig classifierConf,
                       String datasetPath, ExplanationSettings explanationSettings) {
        this.columns = columns;
        this.classifierConf = classifierConf;
        this.datasetPath = datasetPath;
        this.explanationSettings = explanationSettings;
    }

    /**
     * This function must be implemented from each explanation algorithm.
     */
    public abstract <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces);

    protected HashSet<Integer> getPointsToExplain() {
        if(explanationSettings.dictatedOutlierMethod()) {
            return pointsToExplain == null ? new HashSet<>(explanationSettings.getDictatedOutliers()) : pointsToExplain;
        }
        // TODO else do the detection and return the outlier points to explain
        return null;
    }

    protected int getDatasetDimensionality() {
        return columns.length;
    }

    protected String subspaceToString(Collection<Integer> subspaceFeatures, double score) {
        return "[" + Joiner.on(" ").join(subspaceFeatures) + "] " + score + ";";
    }

    protected Subspace toSubspace(String subspaceStr) {
        HashSet<Integer> features = new HashSet<>();
        StringTokenizer strtok = new StringTokenizer(subspaceStr, subspaceDelimiter);
        while (strtok.hasMoreTokens()) {
            features.add(Integer.parseInt(strtok.nextToken()));
        }
        return new Subspace(features);
    }

    protected List<HashSet<Integer>> subspacesToFeaturesList(Collection<Subspace> subspaceList) {
        List<HashSet<Integer>> subspacesFeatures = new ArrayList<>();
        for (Subspace subspace : subspaceList) {
            subspacesFeatures.add(subspace.getFeatures());
        }
        return subspacesFeatures;
    }

    protected List<HashSet<Integer>> hicsSubspacesToFeaturesList(Collection<HiCS.HiCSSubspace> subspaceList) {
        List<HashSet<Integer>> subspacesFeatures = new ArrayList<>();
        for (HiCS.HiCSSubspace subspace : subspaceList) {
            subspacesFeatures.add(new HashSet<>(subspace.getFeatures()));
        }
        return subspacesFeatures;
    }

    protected HashSet<Integer> subspaceToSet(String subspace) {
        StringTokenizer strtok = new StringTokenizer(subspace, subspaceDelimiter);
        HashSet<Integer> featureSet = new HashSet<>();
        while (strtok.hasMoreTokens()) {
            featureSet.add(Integer.parseInt(strtok.nextToken()));
        }
        return featureSet;
    }

    protected DataFrame runClassifier(DataFrame input, Subspace subspace) throws Exception {
        if(explanationSettings.invokePythonClassifier())
            return runClassifierPython(input, subspace);
        else
            return runClassifierNative(input, subspace);
    }

    protected Map<String, double[]>  runClassifierInSubspaces(DataFrame input, List<HashSet<Integer>> subspaceList) throws Exception {
        if(explanationSettings.invokePythonClassifier())
            return OutlierDetectorsWrapper.runPythonClassifierOnSubspaces(classifierConf, datasetPath, subspaceList, getDatasetDimensionality(), input.getNumRows());
        else
            return runClassifierInSubspacesNative(input, subspaceList);
    }

    protected List<Pair<String, double[]>> runClassifierExhaustive(DataFrame input, int finalSubspacesDim) throws Exception {
        if(explanationSettings.invokePythonClassifier())
            return OutlierDetectorsWrapper.runPythonClassifierExhaustive(classifierConf, datasetPath, getDatasetDimensionality(), finalSubspacesDim, input.getNumRows());
        else
            return runClassifierNativeExhaustive(input, finalSubspacesDim);
    }

    private DataFrame runClassifierNative(DataFrame input, Subspace subspace) throws Exception{
        DataFrame tmpDataFrame = new DataFrame();
        String[] subColumns = new String[subspace.getDimensionality()];
        int counter = 0;
        for (int featureId : subspace.getFeatures()) {
            tmpDataFrame.addColumn(columns[featureId], input.getDoubleColumn(featureId));
            subColumns[counter++] = columns[featureId];
        }
        Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
        classifier.process(tmpDataFrame);
        return classifier.getResults();
    }

    private DataFrame runClassifierPython(DataFrame input, Subspace subspace) throws Exception{
        double[] points_scores =
                OutlierDetectorsWrapper.runPythonClassifier(classifierConf, datasetPath, subspace.getFeatures(), getDatasetDimensionality(), input.getNumRows());
        DataFrame df = new DataFrame();
        df.addColumn(outputColumnName, points_scores);
        return df;
    }

    private Map<String, double[]> runClassifierInSubspacesNative(DataFrame input, List<HashSet<Integer>> featuresList) throws Exception {
        Map<String, double[]> subspacesScores = new HashMap<>();
        int subspaceCounter = 1;
        for (HashSet<Integer> features : featuresList) {
            System.out.print("\rMake Detection in: " + featuresList + " (" + (subspaceCounter++) + "/"  + featuresList.size() + ")");
            double[] scores = runClassifierNative(input, new Subspace(features)).getDoubleColumnByName(outputColumnName);
            subspacesScores.put(features.toString(), scores);
        }
        return subspacesScores;
    }

    private List<Pair<String, double[]>> runClassifierNativeExhaustive(DataFrame input, int finalSubspacesDim) throws Exception {
        List<Pair<String, double[]>> subspacesPointsScores = new ArrayList<>();
        for(Subspace subspace : featureCombinations(finalSubspacesDim)) {
            DataFrame results = runClassifierNative(input, subspace);
            subspacesPointsScores.add(new Pair<>(subspace.getFeatures().toString(), results.getDoubleColumnByName(outputColumnName)));
        }
        return subspacesPointsScores;
    }

    private List<Subspace> featureCombinations(int finalSubspacesDim) {
        List<Subspace> features2dComb = features2dCombinations();
        if(finalSubspacesDim == 2)
            return features2dComb;
        else
            return featuresMultiCombinations(features2dComb, finalSubspacesDim);
    }

    private List<Subspace> features2dCombinations() {
        int datasetDim = getDatasetDimensionality();
        List<Subspace> subspacesCombinationList = new ArrayList<>();
        for(int i = 0; i < datasetDim; i++){
            for(int j = i+1; j < datasetDim; j++){
                HashSet<Integer> features = new HashSet<>();
                features.add(i);
                features.add(j);
                Subspace newLookOutSubspace = new Subspace(features);
                subspacesCombinationList.add(newLookOutSubspace);
            }
        }
        return subspacesCombinationList;
    }

    private List<Subspace> featuresMultiCombinations(List<Subspace> features2dComb, int finalSubspacesDim) {
        List<Subspace> tmpCombList = new ArrayList<>();
        List<Subspace> subspacesCombinationList = new ArrayList<>(features2dComb);
        HashSet<String> tmpCombStringSet = new HashSet<>();
        for(int i = 2; i < finalSubspacesDim; i++){
            for(Subspace subspace : subspacesCombinationList){
                HashSet<Integer> featuresCombination = subspace.getFeatures();
                for(int featureId = 0; featureId < getDatasetDimensionality(); featureId++){
                    if(!featuresCombination.contains(featureId)){
                        HashSet<Integer> updatedFeaturesCombination = new HashSet<>(featuresCombination);
                        updatedFeaturesCombination.add(featureId);
                        if(!tmpCombStringSet.contains(updatedFeaturesCombination.toString())){
                            Subspace newSubspace = new Subspace(updatedFeaturesCombination);
                            tmpCombList.add(newSubspace);
                            tmpCombStringSet.add(updatedFeaturesCombination.toString());
                        }
                    }
                }
            }
            subspacesCombinationList.clear();
            subspacesCombinationList.addAll(tmpCombList);
            tmpCombList.clear();
            tmpCombStringSet.clear();
        }
        return subspacesCombinationList;
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
