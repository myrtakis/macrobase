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
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;

import javax.naming.ldap.PagedResultsControl;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public abstract class Explanation implements Transformer {

    protected   String[]            columns;
    protected   AlgorithmConfig     classifierConf;
    protected   String              datasetPath;
    protected   String              outputColumnName        = "_OUTLIER";
    protected   String              relSubspaceColumnName   = "__REL_SUBSPACES";
    private     ExplanationSettings explanationSettings;
    private int classifierRunRepeat;
    private     HashSet<Integer>    pointsToExplain;
    private     boolean             invokePythonClassifier;

    private final String subspaceDelimiter = " .,-\t\n[]{}";

    public Explanation(String[] columns, AlgorithmConfig classifierConf, String datasetPath,
                       ExplanationSettings explanationSettings, int classifierRunRepeat) {
        this.columns = columns;
        this.classifierConf = classifierConf;
        this.datasetPath = datasetPath;
        this.explanationSettings = explanationSettings;
        this.classifierRunRepeat = classifierRunRepeat < 0 ? 1 : classifierRunRepeat;
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

    private Map<String, double[]> refineSubspaceStrings(Map<String, double[]> subspacesScores) {
        Map<String, double[]> refinedSubspaceScores = new HashMap<>();
        for (String subspace : subspacesScores.keySet()) {
            String refinedSubspace = subspaceToSet(subspace).toString();
            refinedSubspaceScores.put(refinedSubspace, subspacesScores.get(subspace));
        }
        return refinedSubspaceScores;
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

    protected Map<String, double[]> runClassifierInSubspaces(DataFrame input, List<HashSet<Integer>> subspaceList) throws Exception {
        if (subspaceList.isEmpty())
            return new HashMap<>();
        if(explanationSettings.invokePythonClassifier()) {
            Map<String, double[]> subspaceScores =
                    OutlierDetectorsWrapper.runPythonClassifierOnSubspaces(classifierConf, classifierRunRepeat,
                            datasetPath, subspaceList, getDatasetDimensionality(), input.getNumRows());
            return refineSubspaceStrings(subspaceScores);
        }
        else
            return runClassifierInSubspacesNative(input, subspaceList);
    }

    protected List<Pair<String, double[]>> runClassifierExhaustive(DataFrame input, int finalSubspacesDim) throws Exception {
        if(explanationSettings.invokePythonClassifier())
            return OutlierDetectorsWrapper.runPythonClassifierExhaustive(classifierConf, classifierRunRepeat, datasetPath,
                    getDatasetDimensionality(), finalSubspacesDim, input.getNumRows());
        else
            return runClassifierNativeExhaustive(input, finalSubspacesDim);
    }

    protected List<Set<Integer>> featureIDsCombinations(Set<Integer> featureIDs, int numOfElementsInComb) {
        int[] arr = featureIDs.stream().mapToInt(Integer::intValue).toArray();
        int[] tmpArr = new int[numOfElementsInComb];
        List<Set<Integer>> featuresComb = new ArrayList<>();
        combinationUtil(arr, tmpArr, featuresComb, 0, arr.length - 1, 0, numOfElementsInComb);
        return featuresComb;
    }

    private void combinationUtil(int arr[], int data[], List<Set<Integer>> featuresComb,
                                 int start, int end, int index, int numOfElementsInComb) {
        // Current combination is ready to be printed, print it
        if (index == numOfElementsInComb) {
            featuresComb.add(new HashSet<>(Ints.asList(data)));
            return;
        }
        // replace index with all possible elements. The condition "end-i+1 >= r-index" makes sure that including one element
        // at index will make a combination with remaining elements at remaining positions
        for (int i = start; i <= end && end - i + 1 >= numOfElementsInComb - index; i++) {
            data[index] = arr[i];
            combinationUtil(arr, data, featuresComb, i+1, end, index+1, numOfElementsInComb);
        }
    }

    private DataFrame runClassifierNative(DataFrame input, Subspace subspace) throws Exception{
        DataFrame tmpDataFrame = new DataFrame();
        String[] subColumns = new String[subspace.getDimensionality()];
        int counter = 0;
        for (int featureId : subspace.getFeatures()) {
            tmpDataFrame.addColumn(columns[featureId], input.getDoubleColumn(featureId));
            subColumns[counter++] = columns[featureId];
        }
        DataFrame outputDf = input.copy();
        double[] finalScores = new double[input.getNumRows()];
        Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns, null);
        for (int rep = 0; rep < classifierRunRepeat; rep++) {
            classifier.process(tmpDataFrame);
            double[] scores = classifier.getResults().getDoubleColumnByName(outputColumnName);
            finalScores = addDoubleArrays(finalScores, scores);
        }
        finalScores = avgArray(finalScores, classifierRunRepeat);
        // printSortedIndexes(finalScores);
        outputDf.addColumn(outputColumnName, finalScores);
        return outputDf;
    }

    private DataFrame runClassifierPython(DataFrame input, Subspace subspace) throws Exception{
        double[] points_scores =
                OutlierDetectorsWrapper.runPythonClassifier(classifierConf, classifierRunRepeat, datasetPath, subspace.getFeatures(),
                        getDatasetDimensionality(), input.getNumRows());
        DataFrame df = new DataFrame();
        df.addColumn(outputColumnName, points_scores);
        return df;
    }

    private Map<String, double[]> runClassifierInSubspacesNative(DataFrame input, List<HashSet<Integer>> featuresList) throws Exception {
        Map<String, double[]> subspacesScores = new HashMap<>();
        int subspaceCounter = 1;
        for (HashSet<Integer> features : featuresList) {
            System.out.print("\r> (java) Scoring Subspace: " + features + " (" + (subspaceCounter++) + "/"  + featuresList.size() + ")");
            double[] scores = runClassifierNative(input, new Subspace(features)).getDoubleColumnByName(outputColumnName);
            subspacesScores.put(features.toString(), scores);
        }
        System.out.println();
        return subspacesScores;
    }

    private List<Pair<String, double[]>> runClassifierNativeExhaustive(DataFrame input, int finalSubspacesDim) throws Exception {
        List<Pair<String, double[]>> subspacesPointsScores = new ArrayList<>();
        int subspaceCounter = 1;
        List<Subspace> combs = featureCombinations(finalSubspacesDim);
        for(Subspace subspace : combs) {
            System.out.print("\r> (java) Scoring Subspace: " + subspace.getFeatures() + " (" + (subspaceCounter++) + "/"  + combs.size() + ")");
            DataFrame results = runClassifierNative(input, subspace);
            subspacesPointsScores.add(new Pair<>(subspace.getFeatures().toString(), results.getDoubleColumnByName(outputColumnName)));
        }
        System.out.println();
        return subspacesPointsScores;
    }

    private List<Subspace> featureCombinations(int finalSubspacesDim) {
        List<int[]> combinations = new ArrayList<>();
        List<Subspace> subspaceCombinations = new ArrayList<>();
        helper(combinations, new int[finalSubspacesDim], 0, getDatasetDimensionality()-1, 0);
        for (int[] comb : combinations) {
            subspaceCombinations.add(new Subspace(new HashSet<>(Ints.asList(comb))));
        }
        return subspaceCombinations;
    }

    private void helper(List<int[]> combinations, int data[], int start, int end, int index) {
        if (index == data.length) {
            int[] combination = data.clone();
            combinations.add(combination);
        } else if (start <= end) {
            data[index] = start;
            helper(combinations, data, start + 1, end, index + 1);
            helper(combinations, data, start + 1, end, index);
        }
    }

    private double[] addDoubleArrays(double[] arr1, double[] arr2) {
        int len = Math.min(arr1.length, arr2.length);
        double[] mergedArray = new double[len];
        for (int i=0; i < len; i++) {
            mergedArray[i] = arr1[i] + arr2[i];
        }
        return mergedArray;
    }

    private double[] avgArray(double[] arr, int n) {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = arr[i] / n;
        }
        return arr;
    }

    private void printSortedIndexes(double[] scores) {
        final Integer[] idx = new Integer[scores.length];
        for (int i = 0; i < scores.length; i++) {
            idx[i] = i;
        }
        Arrays.sort(idx, Comparator.comparingDouble(o -> scores[o]));
        for (int i = 0;  i < 10; i++){
            System.out.print(idx[idx.length - (i+1)] + " ");
        }
        System.out.println();
    }

    protected Path pathForLogging(String explainer) {
        return Paths.get("alexp",  "ExplanationLogs",
                explainer,
                classifierConf.getAlgorithmId(),
                FilenameUtils.removeExtension(FilenameUtils.getBaseName(datasetPath)));
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
