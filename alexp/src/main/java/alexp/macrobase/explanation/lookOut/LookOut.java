package alexp.macrobase.explanation.lookOut;

import alexp.macrobase.explanation.utils.Subspace;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import com.google.common.collect.*;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import javafx.util.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LookOut extends Explanation {

    private int budget; // How many plots (i.e. each plot consists of a dmax number of features) the algorithm will return. Default value is 6 plots
    private int dimensionality; // Indicates the getDimensionality of the subspaces where the lookOut will examine to find the best dmax dimensional subspace. Defaul value is 2 dimensional subspaces

    private DataFrame output;

    /*
        CONSTRUCTORS
     */

    public LookOut(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) throws MacroBaseException {
        super(columns, classifierConf, explanationSettings);
    }

    /*
        OVERRIDE FUNCTIONS
     */

    @Override
    public void process(DataFrame input) throws Exception {
        output = input.copy();
        List<LookOutSubspace> bestSubspaces = new ArrayList<>();
        calculateSubspaces(input, bestSubspaces);
        HashMap<Integer, Double> pointsOfInterestAvgScores = getAvgPointsScores(bestSubspaces);
        double[] scores = new double[input.getNumRows()];
        for(int i = 0; i < scores.length; i++){
            if(getPointsToExplain().contains(i)){
                scores[i] = pointsOfInterestAvgScores.get(i);
            } else {
                scores[i] = 0;
            }
        }
        output.addColumn(outputColumnName, scores);
        addRelSubspaceColumnToDataframe(output, bestSubspaces);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    @Override
    public <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces) {
        List<LookOutSubspace> bestSubspaces = (List<LookOutSubspace>) pointsSubspaces;
        String[] relSubspaceCol = new String[data.getNumRows()];
        for(int pointId = 0; pointId < relSubspaceCol.length; pointId++) {
            if(getPointsToExplain().contains(pointId)) {
                relSubspaceCol[pointId] = "";
            }
            else {
                relSubspaceCol[pointId] = "-";
            }
        }
        for(LookOutSubspace subspace : bestSubspaces) {
            HashMap<Integer, Double> pointsScores = subspace.getPointsOfInterestScores();
            for(int pointId : pointsScores.keySet()) {
                relSubspaceCol[pointId] += subspaceToString(new ArrayList<>(subspace.getFeatures()), pointsScores.get(pointId)) + " ";
            }
        }
        data.addColumn(relSubspaceColumnName, relSubspaceCol);
    }

    /*
        UTIL FUNCTIONS
     */

    private void calculateSubspaces(DataFrame input, List<LookOutSubspace> bestSubspaces) throws Exception {
        Set<LookOutSubspace> allSubspaces = pointsOfInterestScores(input);
        HashMap<Integer, Double> maxOutlierScores = new HashMap<>();
        initMaxOutlierScores(maxOutlierScores);
        int counter = 0;
        while (counter < budget) {
            LookOutSubspace maxMarginalGainSubspace = new LookOutSubspace();
            int currSubspaceCounter = 0;
            for(LookOutSubspace subspace : allSubspaces) {
                System.out.print("\rMaking detection in subspace (" + (++currSubspaceCounter) + "/" + allSubspaces.size() + ")" + subspace);
                double currMarginalGain = calculateMarginalGain(subspace.getPointsOfInterestScores(), maxOutlierScores);
                if(maxMarginalGainSubspace.getScore() < currMarginalGain) {
                    maxMarginalGainSubspace = new LookOutSubspace(subspace);
                    maxMarginalGainSubspace.setScore(currMarginalGain);
                }
            }
            allSubspaces.remove(maxMarginalGainSubspace);
            bestSubspaces.add(maxMarginalGainSubspace);
            patchMaxOutlierScores(maxOutlierScores, maxMarginalGainSubspace.getPointsOfInterestScores());
            counter++;
        }
    }

    private Set<LookOutSubspace> pointsOfInterestScoresTest() {
        Set<LookOutSubspace> s = new HashSet<>();
        LookOutSubspace s1 = new LookOutSubspace(Sets.newHashSet(1,2));
        LookOutSubspace s2 = new LookOutSubspace(Sets.newHashSet(1,3));
        LookOutSubspace s3 = new LookOutSubspace(Sets.newHashSet(2,3));
        s1.setPointsScores(new HashMap<Integer, Double>() {{put(1,0.9); put(2,0.8); put(3,0.8); put(4,0.4);}});
        s2.setPointsScores(new HashMap<Integer, Double>() {{put(1,0.8); put(2,0.7); put(3,0.6); put(4,0.6);}});
        s3.setPointsScores(new HashMap<Integer, Double>() {{put(1,0.7); put(2,0.5); put(3,0.5); put(4,0.8);}});
        s.add(s1);
        s.add(s2);
        s.add(s3);
        return s;
    }

    private Set<LookOutSubspace> pointsOfInterestScores(DataFrame input) throws Exception {
        Set<LookOutSubspace> allSubspaces = Collections.synchronizedSet(new HashSet<>());
        for(LookOutSubspace comb : featureCombinations(dimensionality)) {
            DataFrame tmpDataFrame = new DataFrame();
            LookOutSubspace currSubspace = new LookOutSubspace(comb);
            int counter = 0;
            String[] subColumns = new String[comb.getDimensionality()];
            for(Integer featureId : comb.getFeatures()) {
                tmpDataFrame.addColumn(columns[featureId], input.getDoubleColumn(featureId));
                subColumns[counter++] = columns[featureId];
            }
            Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
            classifier.process(tmpDataFrame);
            double[] pointsScores = classifier.getResults().getDoubleColumnByName(outputColumnName);
            patchPointsOfInterestScores(pointsScores, currSubspace, allSubspaces);
        }
        return allSubspaces;
    }

    private void patchPointsOfInterestScores(double[] pointsScores, LookOutSubspace currSubspace, Set<LookOutSubspace> allSubspaces) {
        for(int pointId = 0; pointId < pointsScores.length; pointId++){
            if(super.getPointsToExplain().contains(pointId)){
                currSubspace.addPointAndScore(pointId, pointsScores[pointId]);
            }
        }
        allSubspaces.add(currSubspace);
    }

    private void initMaxOutlierScores(Map<Integer, Double> maxOutlierScores) {
        for(int pointId : getPointsToExplain()) {
            maxOutlierScores.put(pointId, 0.0);
        }
    }
    
    private void patchMaxOutlierScores(Map<Integer, Double> maxOutlierScores, Map<Integer, Double> currOutlierScores) {
        if(maxOutlierScores.size() != currOutlierScores.size())
            throw new RuntimeException("maxOutlierScores must have same size with currOutlierScores " + maxOutlierScores.size() + " != " + currOutlierScores.size());
        for(int pointId : maxOutlierScores.keySet()) {
            maxOutlierScores.put(pointId, Math.max(maxOutlierScores.get(pointId), currOutlierScores.get(pointId)));
        }
    }

    private double outlierScoresSum(HashMap<Integer, Double> outlierScores) {
        double sum = 0;
        for(int pointId : outlierScores.keySet()) {
            sum += outlierScores.get(pointId);
        }
        return sum;
    }

    private double calculateMarginalGain(HashMap<Integer, Double> currPointsScores, HashMap<Integer, Double> maxPointsScores) {
        if(currPointsScores.size() != maxPointsScores.size())
            throw new RuntimeException("Two maps don't have the same size " + currPointsScores.size() + " != " + maxPointsScores.size());
        double maxScoreSum = outlierScoresSum(maxPointsScores);
        double newScoreSum = 0;
        double marginalGain;
        for(int pointId : currPointsScores.keySet()){
            newScoreSum += Math.max(currPointsScores.get(pointId), maxPointsScores.get(pointId));
        }
        marginalGain = newScoreSum - maxScoreSum;
        if(marginalGain < 0)
            throw new RuntimeException("Marginal gain must be >= 0 " + marginalGain);
        return marginalGain;
    }

    private List<LookOutSubspace> featureCombinations(int dimensionality) {
        int datasetDim = getDatasetDimensionality();
        List<LookOutSubspace> subspacesCombinationList = new ArrayList<>();
        for(int i = 0; i < datasetDim; i++){
            for(int j = i+1; j < datasetDim; j++){
                HashSet<Integer> features = new HashSet<>();
                features.add(i);
                features.add(j);
                LookOutSubspace newLookOutSubspace = new LookOutSubspace(features);
                subspacesCombinationList.add(newLookOutSubspace);
            }
        }
        List<LookOutSubspace> tmpCombList = new ArrayList<>();
        HashSet<String> tmpCombStringSet = new HashSet<>();
        for(int i = 2; i < dimensionality; i++){
            for(LookOutSubspace lookOutSubspace : subspacesCombinationList){
                HashSet<Integer> featuresCombination = lookOutSubspace.getFeatures();
                for(int featureId = 0; featureId < getDatasetDimensionality(); featureId++){
                    if(!featuresCombination.contains(featureId)){
                        HashSet<Integer> updatedFeaturesCombination = new HashSet<>(featuresCombination);
                        updatedFeaturesCombination.add(featureId);
                        if(!tmpCombStringSet.contains(updatedFeaturesCombination.toString())){
                            LookOutSubspace newLookOutSubspace = new LookOutSubspace(updatedFeaturesCombination);
                            tmpCombList.add(newLookOutSubspace);
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

    private HashMap<Integer, Double> getAvgPointsScores(List<LookOutSubspace> bestSubspaces) {
        HashMap<Integer, Double> avgPointsScores = new HashMap<>();
        for(LookOutSubspace subspace : bestSubspaces) {
            HashMap<Integer, Double> pointsOfInterestScores = subspace.getPointsOfInterestScores();
            for(int pointId : pointsOfInterestScores.keySet()) {
                double oldScore = avgPointsScores.getOrDefault(pointId, 0.0);
                double newscore = pointsOfInterestScores.get(pointId);
                avgPointsScores.put(pointId, oldScore + newscore);
            }
        }
        for(int pointId : avgPointsScores.keySet()) {
            avgPointsScores.put(pointId, avgPointsScores.get(pointId) / bestSubspaces.size());
        }
        return avgPointsScores;
    }
    /*
        SETTER FUNCTIONS
     */

    public void setBudget(int budget) {
        this.budget = budget;
    }

    public void setDimensionality(int dimensionality) {
        this.dimensionality = dimensionality;
    }

    private static class LookOutSubspace extends Subspace{
        HashMap<Integer, Double> pointsOfInterestScores = new HashMap<>();

        public LookOutSubspace() {
        }

        public LookOutSubspace(HashSet<Integer> features) {
            super(features);
        }

        public LookOutSubspace(LookOutSubspace subspaceToCopy) {
            super(subspaceToCopy);
            this.pointsOfInterestScores.putAll(subspaceToCopy.getPointsOfInterestScores());
        }

        public void setPointsScores(HashMap<Integer, Double> pointsOfInterestScores) {
            this.pointsOfInterestScores = pointsOfInterestScores;
        }

        public void addPointAndScore(int pointId, double score) {
            pointsOfInterestScores.put(pointId, score);
        }

        public HashMap<Integer, Double> getPointsOfInterestScores() {
            return pointsOfInterestScores;
        }

        public double getPointScore(int pointId) {
            return pointsOfInterestScores.get(pointId);
        }
    }

}
