package alexp.macrobase.explanation.beam;

import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.explanation.utils.Subspace;
import alexp.macrobase.explanation.utils.datastructures.heap.Heap;
import alexp.macrobase.explanation.utils.datastructures.heap.TopBoundedHeap;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.org.apache.xml.internal.utils.SuballocatedByteVector;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import it.unimi.dsi.fastutil.Hash;
import javafx.util.Pair;

import java.util.*;

public class BeamSubspaceSearch extends Explanation {

    /**
     * The number of the top-k subspaces that should be kept.
     */
    private int topk;

    /**
     * The maximum getDimensionality.
     */
    private int dmax;

    /**
     * The variant of Beam when the algorithm returns subspaces of fixed dimensionality.
     */
    private boolean beamFixed;

    /**
     * The beam width.
     */
    private int W;

    /**
     * The output DataFrame.
     */
    private DataFrame output;


    /**
     * Store in every stage the computed subspace scores for each point of interest. That means, you don't have to compute
     * scores of the outliers in subspaces that are already computed
     */
    private HashMap<String, HashMap<Integer, Double>> consideredSubspacesScores = new HashMap<>();


    public BeamSubspaceSearch(String[] columns, AlgorithmConfig classifierConf, String datasetPath,
                              ExplanationSettings explanationSettings, int classifierRunRepeat) {
        super(columns, classifierConf, datasetPath, explanationSettings, classifierRunRepeat);
    }


    @Override
    public void process(DataFrame input) throws Exception {
        output = input.copy();
        HashMap<Integer, List<Subspace>> pointsSubspaces =  calculateSubspaces2(input);
        double[] scores = new double[input.getNumRows()];
        for(int pointId : getPointsToExplain()) {
            scores[pointId] = getAvgScore(pointsSubspaces.get(pointId));
        }
        output.addColumn(outputColumnName, scores);
        addRelSubspaceColumnToDataframe(output, pointsSubspaces);
    }


    @Override
    public <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces) {
        HashMap<Integer, List<Subspace>> pointsScores = (HashMap<Integer, List<Subspace>>) pointsSubspaces;
        String[] relSubspaces = new String[data.getNumRows()];
        for (int i = 0; i < relSubspaces.length; i++) {
            if (pointsScores.containsKey(i)) {
                StringBuilder sb = new StringBuilder();
                for (Subspace subspace : pointsScores.get(i)) {
                    sb.append(subspaceToString(subspace.getFeatures(), subspace.getScore())).append(" ");
                }
                relSubspaces[i] = sb.toString().trim();
            } else {
                relSubspaces[i] = "-";
            }
        }
        data.addColumn(relSubspaceColumnName, relSubspaces);
    }


    @Override
    public DataFrame getResults() {
        return output;
    }


    private HashMap<Integer, List<Subspace>> calculateSubspaces2(DataFrame input) throws Exception {
        HashMap<Integer, List<Subspace>> pointsScores = new HashMap<>();
        HashMap<Integer, TopBoundedHeap<Subspace>> results = score2dSubspaces(input);

        if (dmax > 2) {
            results = scorePointInMultiDimSubspaces2(results, input);
        }

        for(int pointId : results.keySet()) {
            List<Subspace> topSubspaces = results.get(pointId).topK(topk, Subspace.SORT_BY_SCORE_DESC);
            pointsScores.put(pointId, topSubspaces);
        }

        return pointsScores;
    }


    private HashMap<Integer, TopBoundedHeap<Subspace>> scorePointInMultiDimSubspaces2(
            HashMap<Integer, TopBoundedHeap<Subspace>> best2dSubspaces, DataFrame input) throws Exception {

        HashMap<Integer, TopBoundedHeap<Subspace>> globalPointsList = new HashMap<>();
        HashMap<Integer, TopBoundedHeap<Subspace>> stagePointsList = new HashMap<>();

        for (int i = 3; i <= dmax; i++) {
            int counter = 0;
            System.out.println("\n---------------- STAGE " + i + " ----------------");
            for (int pointId : best2dSubspaces.keySet()) {
                if (!globalPointsList.containsKey(pointId)) {
                    globalPointsList.put(pointId, new TopBoundedHeap<>(topk, Subspace.SORT_BY_SCORE_ASC));
                }
                if (!stagePointsList.containsKey(pointId)) {
                    stagePointsList.put(pointId, new TopBoundedHeap<>(best2dSubspaces.get(pointId)));
                }
                List<Subspace> newSubspaces = getNewSubspaces(stagePointsList.get(pointId), i);
                stagePointsList.get(pointId).clear();
                if (!stagePointsList.get(pointId).isEmpty())
                    throw new RuntimeException("Point " + pointId + ": Stage points list should be cleared, " + stagePointsList.get(pointId).size());
                List<HashSet<Integer>> nonComputedSubspaceList = nonComputedSubspaces(newSubspaces);
                String consoleOutput = "(" + (++counter) + "/" + best2dSubspaces.keySet().size()+ ") Scoring point " + pointId;
                consoleOutput = nonComputedSubspaceList.isEmpty() ? "\r" + consoleOutput : "\n" + consoleOutput + "\n";
                System.out.print(consoleOutput);
                Map<String, double[]> subspacesScores = runClassifierInSubspaces(input, nonComputedSubspaceList);
                updateComputedSubspaces(subspacesScores);
                for (Subspace subspace : newSubspaces) {
                    Subspace poiSubspace = new Subspace(subspace);
                    Double poiScore = consideredSubspacesScores.get(subspace.getFeatures().toString()).get(pointId);
                    poiSubspace.setScore(poiScore);
                    stagePointsList.get(pointId).add(poiSubspace);
                    globalPointsList.get(pointId).add(poiSubspace);
                }
            }
            consideredSubspacesScores.clear();
        }
        return beamFixed ? stagePointsList : globalPointsList;
    }


/*
    private HashMap<Integer, List<Subspace>> calculateSubspaces(DataFrame input) throws Exception {
        HashMap<Integer, List<Subspace>> pointsScores = new HashMap<>();

        HashMap<Integer, TopBoundedHeap<Subspace>> points2dScores = score2dSubspaces(input);
        int counter = 0;
        for(int pointId : points2dScores.keySet()) {
            List<Subspace> topSubspaces;
            if(dmax > 2){
                topSubspaces = scorePointInMultiDimSubspaces(input, points2dScores.get(pointId), pointId, ++counter);
                topSubspaces.sort(Subspace.SORT_BY_SCORE_DESC);
            } else {
                topSubspaces = points2dScores.get(pointId).topK(topk, Subspace.SORT_BY_SCORE_DESC);
            }
            pointsScores.put(pointId, topSubspaces);
        }
        return pointsScores;
    }


    private List<Subspace> scorePointInMultiDimSubspaces(DataFrame input, TopBoundedHeap<Subspace> best2dSubspaces,
                                                         int pointId, int counter) throws Exception {
        TopBoundedHeap<Subspace> globalList = new TopBoundedHeap<>(topk, Subspace.SORT_BY_SCORE_ASC);
        TopBoundedHeap<Subspace> stageList = new TopBoundedHeap<>(best2dSubspaces);
        for (int i = 3; i <= dmax; i++) {
            List<Subspace> newSubspaces = getNewSubspaces(stageList, i);
            stageList.clear();
            List<HashSet<Integer>> nonComputedSubspaceList = nonComputedSubspaces(newSubspaces);
            if(nonComputedSubspaceList.isEmpty()){
                System.out.print("\rScoring point " + counter + " / " + getPointsToExplain().size());
            } else {
                System.out.println("\nScoring point " + counter + " / " + getPointsToExplain().size());
            }
            Map<String, double[]> subspacesScores = runClassifierInSubspaces(input, nonComputedSubspaceList);
            updateComputedSubspaces(subspacesScores);
            for (Subspace subspace : newSubspaces) {
                Subspace poiSubspace = new Subspace(subspace);
                Double poiScore = consideredSubspacesScores.get(subspace.getFeatures().toString()).get(pointId);
                poiSubspace.setScore(poiScore);
                stageList.add(poiSubspace);
                globalList.add(poiSubspace);
            }
        }
        // Check which of the two Beam variations will be triggered
        return beamFixed ? stageList.toList() : globalList.toList();
    }
*/

    private List<Subspace> getNewSubspaces(TopBoundedHeap<Subspace> stageCandidates, int stage) {
        List<Subspace> stageCandList = stageCandidates.toList();
        Set<Subspace> newSubspaceSet = new HashSet<>();
        stageCandList.sort(Subspace.SORT_BY_FEATURES);
        // TODO TEST THAT IN HELPER MAIN
        for (Subspace subspace : stageCandList) {
            for (int featureId = 0; featureId < getDatasetDimensionality(); featureId++) {
                HashSet<Integer> mergedFeatures = new HashSet<>(subspace.getFeatures());
                mergedFeatures.add(featureId);
                if (mergedFeatures.size() == stage)
                    newSubspaceSet.add(new Subspace(mergedFeatures));
            }
        }
        if (newSubspaceSet.size() > stageCandidates.size() * ((getDatasetDimensionality() - stage) + 1))
            throw new RuntimeException("More combinations than it should be. Stage candidates = " + stageCandidates.size() +
                    " and combinations " + newSubspaceSet.size() + "\n" + newSubspaceSet);
        return new ArrayList<>(newSubspaceSet);
    }


    private List<HashSet<Integer>> nonComputedSubspaces(List<Subspace> subspaceList) {
        List<HashSet<Integer>> nonComputedSubs = new ArrayList<>();
        for (Subspace subspace : subspaceList) {
            if (!consideredSubspacesScores.containsKey(subspace.getFeatures().toString()))
                nonComputedSubs.add(subspace.getFeatures());
        }
        return nonComputedSubs;
    }


    private void updateComputedSubspaces(Map<String, double[]> subspacesScores) {
        for (String subspaceStr : subspacesScores.keySet()) {
            if (consideredSubspacesScores.containsKey(subspaceStr))
                throw new RuntimeException("Subspace " + subspaceStr + " is already considered");
            double[] scores = subspacesScores.get(subspaceStr);
            HashMap<Integer, Double> poiScoresInSubspace = new HashMap<>();
            for (int poiID : getPointsToExplain()) {
                poiScoresInSubspace.put(poiID, scores[poiID]);
            }
            consideredSubspacesScores.put(subspaceStr, poiScoresInSubspace);
        }
    }


    private HashMap<Integer, TopBoundedHeap<Subspace>> score2dSubspaces(DataFrame input) throws Exception {
        System.out.println("Calculating scores in 2-dimensional projections exhaustively...");
        HashMap<Integer, TopBoundedHeap<Subspace>> pointsScores = new HashMap<>();
        int dim = 2;
        List<Pair<String, double[]>> subspacesPointsScores = runClassifierExhaustive(input, dim);
        for (Pair<String, double[]> pair : subspacesPointsScores) {
            double[] scores = pair.getValue();
            String subspaceStr = pair.getKey();
            for (int poiID : getPointsToExplain()) {
                TopBoundedHeap<Subspace> bestPointSubspaces = new TopBoundedHeap<>(W, Subspace.SORT_BY_SCORE_ASC);
                bestPointSubspaces = pointsScores.getOrDefault(poiID, bestPointSubspaces);
                Subspace subspace = new Subspace(subspaceToSet(subspaceStr));
                subspace.setScore(scores[poiID]);
                bestPointSubspaces.add(subspace);
                pointsScores.put(poiID, bestPointSubspaces);
            }
        }
        return pointsScores;
    }


    /*
        UTIL FUNCTIONS
     */


    private boolean featuresInTopScored(HashSet<String> topScoredFeatures, Subspace newSubspace) {
        return topScoredFeatures.contains(newSubspace.getFeatures().toString());
    }


    private double getAvgScore(List<Subspace> pointTopKsubspaces) {
        return pointTopKsubspaces.stream().mapToDouble(Subspace::getScore).sum()/pointTopKsubspaces.size();
    }

    /*
        SETTER FUNCTIONS
     */

    public void setTopk(int topk) {
        this.topk = topk;
    }


    public void setDmax(int dmax) {
        this.dmax = dmax;
    }


    public void setW(int w) {
        W = w;
    }


    public void setBeamFixed(boolean beamFixed) { this.beamFixed = beamFixed; }


}
