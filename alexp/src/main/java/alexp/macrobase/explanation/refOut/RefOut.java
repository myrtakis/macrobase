package alexp.macrobase.explanation.refOut;

import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.explanation.hics.statistics.tests.GoodnessOfFitTest;
import alexp.macrobase.explanation.hics.statistics.tests.WelchTTest;
import alexp.macrobase.explanation.utils.Subspace;
import alexp.macrobase.explanation.utils.datastructures.heap.Heap;
import alexp.macrobase.explanation.utils.datastructures.heap.TopBoundedHeap;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import com.github.chen0040.data.utils.TupleTwo;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;
import java.util.stream.Collectors;

import javafx.util.Pair;


public class RefOut extends Explanation {

    private double  d1;
    private int     d2;
    private int     psize;
    private int     beamSize;
    private int     topk;

    GoodnessOfFitTest statTest = new WelchTTest();

    /**
     * The output DataFrame.
     */
    private DataFrame output;

    public RefOut(String[] columns, AlgorithmConfig classifierConf, String datasetPath,
                  ExplanationSettings explanationSettings, int classifierRunRepeat) {
        super(columns, classifierConf, datasetPath, explanationSettings, classifierRunRepeat);
    }

    /*
        OVERRIDE FUNCTIONS
     */

    @Override
    public void process(DataFrame input) throws Exception {
        output = input.copy();
        List<Subspace> refinedPool = calculateSubspaces(input);
        HashMap<Integer, List<Subspace>> pointsScores = scorePointsInRefinedPool(input, refinedPool);
        double[] avgScores = new double[input.getNumRows()];
        for(int pointId : pointsScores.keySet()) {
            double avgScore = pointsScores.get(pointId).stream().mapToDouble(Subspace::getScore).sum() / pointsScores.get(pointId).size();
            avgScores[pointId] = avgScore;
        }
        output.addColumn(outputColumnName, avgScores);
        addRelSubspaceColumnToDataframe(output, pointsScores);
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

    /*
    *   HELPER FUNCTIONS
    * */

    private double[] normalizeScores(double[] rawScores) {
        double[] normalizedScores = new double[rawScores.length];
        double sum = 0;
        double mean;
        double N = rawScores.length;
        double var;
        for(double score : rawScores) {
            sum += score;
        }
        mean = sum / N;
        sum = 0;
        for(double score : rawScores) {
            sum += Math.pow(score - mean, 2);
        }
        var = sum / (N - 1);
        for (int i = 0; i < rawScores.length; i++) {
            normalizedScores[i] = (rawScores[i] - mean) / Math.sqrt(var);
        }
        return normalizedScores;
    }

    private HashMap<Integer, List<Subspace>> scorePointsInRefinedPool(DataFrame input, List<Subspace> refinedPool) throws Exception {
        System.out.println("Score all refined subspaces from the refined pool");
        HashMap<Integer, List<Subspace>> pointsScores = getPointsOfInterestScoresInSubspaces(input, refinedPool);
        for (int pointId : pointsScores.keySet()) {
            List<Subspace> topkList = new ArrayList<>(pointsScores.get(pointId));
            topkList.sort(Subspace.SORT_BY_SCORE_DESC);
            pointsScores.put(pointId, topkList.subList(0, topk));
        }
        return pointsScores;
    }

    private List<Subspace> calculateSubspaces(DataFrame input) throws Exception {
        System.out.println("Score all random subspaces in the pool");
        HashMap<Integer, List<Subspace>> pointsScoresRandomPool = getPointsOfInterestScoresInSubspaces(input, getRandomPool());
        HashSet<Subspace> refinedPool = new HashSet<>();
        for(int pointId : pointsScoresRandomPool.keySet()) {
            Subspace refinedSubspace = refine(pointsScoresRandomPool.get(pointId));
            refinedPool.add(refinedSubspace);
        }
        return new ArrayList<>(refinedPool);
    }

    private List<Subspace> getRandomPool() {
        HashSet<Subspace> P1 = new HashSet<>(psize);
        int card = getDatasetDimensionality();
        double poolSubspaceDim = Math.ceil(card * d1);
        while(P1.size() < psize) {
            HashSet<Integer> randFeatures = new HashSet<>();
            while (randFeatures.size() < poolSubspaceDim) {
                randFeatures.add(new Random().nextInt(card));
            }
            Subspace newRandSubspace = new Subspace(randFeatures);
            P1.add(newRandSubspace);
        }
        return new ArrayList<>(P1);
    }

    private HashMap<Integer, List<Subspace>> getPointsOfInterestScoresInSubspaces(DataFrame input, List<Subspace> subspaceList) throws Exception {
        HashMap<Integer, List<Subspace>> pointsScores = new HashMap<>();
        int counter = 0;
        for(Subspace subspace : subspaceList) {
            System.out.print("\rScoring subspace " + (++counter) + "/" + subspaceList.size());
            DataFrame results = runClassifier(input, subspace);
            double[] scores = normalizeScores(results.getDoubleColumnByName(outputColumnName)); // get the score column from the dataframe and normalize
            for(int pointId : getPointsToExplain()) {
                List<Subspace> pointsSubspaces = pointsScores.getOrDefault(pointId, new ArrayList<>());
                pointsSubspaces.add(new Subspace(subspace.getFeatures(), scores[pointId]));
                pointsScores.put(pointId, pointsSubspaces);
            }
        }
        System.out.println();
        return pointsScores;
    }

    private Subspace refine(List<Subspace> pointScores) {
        TopBoundedHeap<Subspace> topD2Subspaces = beamSearch(pointScores);
        return topD2Subspaces.topK(topk, Subspace.SORT_BY_SCORE_DESC).get(0);    // returns the subspace with the best quality of dimensionality d2
    }

    private TopBoundedHeap<Subspace> beamSearch(List<Subspace> pointScores) {
        TopBoundedHeap<Subspace> oneDimCandidates = buildOneDimCandidates(pointScores);
        if(d2 == 1) {
            return oneDimCandidates;
        }
        else {
            return buildMultiDimCandidates(pointScores, oneDimCandidates);
        }
    }

    private TopBoundedHeap<Subspace> buildOneDimCandidates(List<Subspace> pointScores) {
        TopBoundedHeap<Subspace> oneDimCandidates = new TopBoundedHeap<>(beamSize, Subspace.SORT_BY_SCORE_ASC);
        for (int featureId = 0; featureId < getDatasetDimensionality(); featureId++) {
            Subspace oneDimCand = new Subspace(Sets.newHashSet(featureId));
            TupleTwo<double[], double[]> partitions = partitionScores(pointScores, oneDimCand.getFeatures());
            oneDimCand.setScore(candidateQuality(partitions));
            oneDimCandidates.add(oneDimCand);
        }
        return oneDimCandidates;
    }

    private TopBoundedHeap<Subspace> buildMultiDimCandidates(List<Subspace> pointScores, TopBoundedHeap<Subspace> oneDimCandidates) {
        TopBoundedHeap<Subspace> multiDimCandidates = new TopBoundedHeap<>(oneDimCandidates);
        for (int stageDim = 2; stageDim <= d2; stageDim++) {
            List<HashSet<Integer>> candidatesFeatures = candidatesFeatures(multiDimCandidates);
            multiDimCandidates.clear();
            HashSet<String> allFeatureCombinations = new HashSet<>();
            for (int i=0; i < candidatesFeatures.size() - 1; i++) {
                for (int j = i + 1; j < candidatesFeatures.size(); j++) {
                    HashSet<Integer> featureCombination = combineCandFeatures(candidatesFeatures, i, j);
                    featureCombination = featureCombination.size() > stageDim ? calculateStageDimCombination(pointScores, featureCombination, stageDim) : featureCombination;
                    if(candidateAlreadyProcessed(allFeatureCombinations, featureCombination)) {
                        continue;
                    }
                    Subspace multiDimCand = new Subspace(featureCombination);
                    TupleTwo<double[], double[]> partitions = partitionScores(pointScores, featureCombination);
                    multiDimCand.setScore(candidateQuality(partitions));
                    multiDimCandidates.add(multiDimCand);
                }
            }
        }
        return multiDimCandidates;
    }

    private double candidateQuality(TupleTwo<double[], double[]> partitions) {
        double[] leftPartition = partitions._1();
        double[] rightPartition = partitions._2();
        double quality = -1;
        if(leftPartition.length > 2 && rightPartition.length > 2) {
            quality = statTest.deviation(leftPartition, rightPartition);  // Welch test will return 1-pvalue -> Lower pvalue means higher quality
        }
        return quality;
    }

    private TupleTwo<double[], double[]> partitionScores(List<Subspace> pointScores, HashSet<Integer> candSubspaceFeatures) {
        List<Double> leftScores = new ArrayList<>();
        List<Double> rightScores = new ArrayList<>();
        for (Subspace subspace : pointScores) {
            if(subspace.getFeatures().containsAll(candSubspaceFeatures)){
                rightScores.add(subspace.getScore());
            } else {
                leftScores.add(subspace.getScore());
            }
        }
        return new TupleTwo<>(Doubles.toArray(leftScores), Doubles.toArray(rightScores));
    }

    private List<HashSet<Integer>> candidatesFeatures(TopBoundedHeap<Subspace> topList) {
        List<HashSet<Integer>> candidatesFeatures = new ArrayList<>();
        for (Heap<Subspace>.UnorderedIter iter = topList.unorderedIter(); iter.valid(); iter.advance()) {
            candidatesFeatures.add(iter.get().getFeatures());
        }
        candidatesFeatures.sort(Comparator.comparing(HashSet::hashCode));
        return candidatesFeatures;
    }

    private HashSet<Integer> combineCandFeatures(List<HashSet<Integer>> candFeatures, int i, int j) {
        HashSet<Integer> set1 = new HashSet<>(candFeatures.get(i));
        HashSet<Integer> set2 = new HashSet<>(candFeatures.get(j));
        set1.addAll(set2);  // combine two sets
        return set1;
    }

    private boolean candidateAlreadyProcessed(HashSet<String> allFeatureCombinations, HashSet<Integer> currFeatureCombination) {
        if(allFeatureCombinations.contains(currFeatureCombination.toString())) {
            return true;
        } else {
            allFeatureCombinations.add(currFeatureCombination.toString());
            return false;
        }
    }

    private HashSet<Integer> calculateStageDimCombination(List<Subspace> pointScores, HashSet<Integer> featureCombination, int stageDim) {
        List<Pair<Integer, Double>> oneDimQuality = new ArrayList<>();
        for (int featureId : featureCombination) {
            TupleTwo<double[], double[]> partitions = partitionScores(pointScores, Sets.newHashSet(featureId));
            oneDimQuality.add(new Pair<>(featureId, candidateQuality(partitions)));
        }
        oneDimQuality.sort(Comparator.comparing(Pair::getValue));
        Collections.reverse(oneDimQuality);
        HashSet<Integer> bestFeatures = oneDimQuality.stream().map(Pair::getKey).limit(3).collect(Collectors.toCollection(HashSet::new));
        return bestFeatures;
    }


    public void setD1(double d1) { this.d1 = d1; }

    public void setD2(int d2) { this.d2 = d2; }

    public void setPsize(int psize) { this.psize = psize; }

    public void setBeamSize(int beamSize) { this.beamSize = beamSize; }

    public void setTopk(int topk) { this.topk = topk; }

}
