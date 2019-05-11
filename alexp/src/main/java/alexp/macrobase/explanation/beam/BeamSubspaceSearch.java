package alexp.macrobase.explanation.beam;

import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.explanation.utils.datastructures.heap.Heap;
import alexp.macrobase.explanation.utils.datastructures.heap.TopBoundedHeap;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;

public class BeamSubspaceSearch extends Explanation {

    /**
     * The number of the top-k subspaces that should be kept.
     */
    private int topk;

    /**
     * The maximum dimensionality.
     */
    private int dmax;

    /**
     * The beam width.
     */
    private int W;

    /**
     * The output DataFrame.
     */
    private DataFrame output;

    public BeamSubspaceSearch(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) {
        super(columns, classifierConf, explanationSettings);
    }

    @Override
    public <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces) {

    }

    @Override
    public void process(DataFrame input) throws Exception {
        output = input.copy();
        TreeMap<Integer, TopBoundedHeap<Subspace>> pointBestSubspaces = new TreeMap<>();
        int counter = 0;
        for(int pointId : getPointsToExplain()){
            System.out.println("Calculating subspaces for point " + pointId + " (" + (++counter) + "/" + getPointsToExplain().size() + ") :");
            calculateSubspaces(input, pointId, pointBestSubspaces);
        }
        double[] scores = new double[input.getNumRows()];
        for(int point = 0; point < scores.length; point++){
            scores[point] = pointBestSubspaces.containsKey(point) ? getAvgScore(pointBestSubspaces.get(point)) : 0.0;
        }
        output.addColumn(outputColumnName, scores);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    private void calculateSubspaces(DataFrame input, int pointId,
                                    TreeMap<Integer, TopBoundedHeap<Subspace>> pointBestSubspaces) throws Exception {
        TopBoundedHeap<Subspace> topKsubspaces = new TopBoundedHeap<>(topk, Subspace.SORT_BY_SCORE_ASC);
        TopBoundedHeap<Subspace> topWsubspaces = new TopBoundedHeap<>(W, Subspace.SORT_BY_SCORE_ASC);
        HashSet<String> topScoredFeatures = new HashSet<>();
        int dim = getDatasetDimensionality();
        // compute two-element sets of subspaces
        score2Dsubspaces(input, dim, topKsubspaces, topWsubspaces, pointId);
        for(int i = 2; i < dmax; i++){
            TopBoundedHeap<Subspace> candidates = new TopBoundedHeap<>(topWsubspaces);  // copy current topWsubspaces heap to candidates heap
            topWsubspaces.clear();
            topScoredFeatures.clear();
            // System.out.println(candidates);
            for(Heap<Subspace>.UnorderedIter it =  candidates.unorderedIter(); it.valid(); it.advance()){
                // System.out.println(it.get().getFeatures());
                for(int featureId = 0; featureId < getDatasetDimensionality(); featureId++){
                    Subspace newSubspace = new Subspace(it.get());  // copy previous subspace to the new one
                    int oldSubspaceDim = newSubspace.dimenionality();
                    newSubspace.setFeature(featureId);
                    //System.out.println("\tAdd feature " + featureId + " to " + it.get().getFeatures() + " -> " + newSubspace.getFeatures());
                    if(newSubspace.dimenionality() == oldSubspaceDim || featuresInTopScored(topScoredFeatures, newSubspace))
                        continue;
                    String[] subColumns = new String[newSubspace.dimenionality()];
                    DataFrame tmpDataFrame = new DataFrame();
                    int counter = 0;
                    for(int subspaceFeatureId : newSubspace.getFeatures()){
                        tmpDataFrame.addColumn(columns[subspaceFeatureId], input.getDoubleColumn(subspaceFeatureId));
                        subColumns[counter++] = columns[subspaceFeatureId];
                    }
                    Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
                    classifier.process(tmpDataFrame);
                    newSubspace.score = classifier.getResults().getDoubleColumnByName(outputColumnName)[pointId];
                    System.out.print("\r\t" + newSubspace);
                    updateTopScoredFeatures(topScoredFeatures, newSubspace, topWsubspaces);
                    topKsubspaces.add(newSubspace);
                    topWsubspaces.add(newSubspace);
                }
            }
        } // end for
        pointBestSubspaces.put(pointId, topKsubspaces);
    }

    private void score2Dsubspaces(DataFrame input, int dim, TopBoundedHeap<Subspace> topKsubspaces,
                                  TopBoundedHeap<Subspace> topWsubspaces, int pointId) throws Exception {
        String[] subColumns = new String[2];
        for(int i = 0; i < dim; i++) {
            for(int j = i + 1; j < dim; j++) {
                Subspace subspace = new Subspace();
                subspace.setFeature(i);
                subspace.setFeature(j);
                DataFrame tmpDataFrame = new DataFrame();
                tmpDataFrame.addColumn(columns[i], input.getDoubleColumn(i));
                tmpDataFrame.addColumn(columns[j], input.getDoubleColumn(j));
                subColumns[0] = columns[i];
                subColumns[1] = columns[j];
                Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
                classifier.process(tmpDataFrame);
                // TODO you can get directly all the points of interest there
                subspace.score = classifier.getResults().getDoubleColumnByName(outputColumnName)[pointId];
                System.out.print("\r\t" + subspace);
                topKsubspaces.add(subspace);
                topWsubspaces.add(subspace);
            }
        }
    }

    /*
        UTIL FUNCTIONS
     */

    private boolean featuresInTopScored(HashSet<String> topScoredFeatures, Subspace newSubspace) {
        return topScoredFeatures.contains(featuresToString(newSubspace.getFeatures()));
    }

    private void updateTopScoredFeatures(HashSet<String> topScoredFeatures, Subspace candidateSubspace,
                                         TopBoundedHeap<Subspace> topWsubspaces) {
        if(topWsubspaces.isEmpty() || topWsubspaces.peek().score < candidateSubspace.score)
            topScoredFeatures.add(featuresToString(candidateSubspace.getFeatures()));
    }

    private String featuresToString(HashSet<Integer> features){
        return Joiner.on("").join(features);
    }

    private String pointTopScoredSubspacesOutput(TopBoundedHeap<Subspace> pointTopKsubspaces) {
        StringBuilder sb = new StringBuilder();
        sb.append("Subspaces: ").append(pointTopKsubspaces).append("\n");
        sb.append("AvgScore: ").append(getAvgScore(pointTopKsubspaces));
        return sb.toString();
    }

    private double getAvgScore(TopBoundedHeap<Subspace> pointTopKsubspaces) {
        double sum = 0;
        for(Heap<Subspace>.UnorderedIter it = pointTopKsubspaces.unorderedIter(); it.valid(); it.advance()) {
            sum += it.get().score;
        }
        return sum/pointTopKsubspaces.size();
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

    /*
        Subspace Class
     */

    private static class Subspace{

        private double score;

        private HashSet<Integer> features;

        public Subspace() {
        }

        public Subspace(Subspace subspaceToCopy) {
            this.features = new HashSet<>(subspaceToCopy.features);
            this.score = subspaceToCopy.score;
        }

        public HashSet<Integer> getFeatures() {
            return features;
        }

        public int dimenionality() {
            return features.size();
        }

        public void setFeature(int featureId) {
            if(features == null)
                features = new HashSet<>();
            features.add(featureId);
        }

        @Override
        public String toString() {
            return "[" + score + ", " + features.toString() + "]";
        }

        /**
         * Sort subspaces by their score in ascending order.
         */
        public static final Comparator<Subspace> SORT_BY_SCORE_ASC = (o1, o2) -> {
            if(o1.score == o2.score) {
                return 0;
            }
            return o1.score > o2.score ? 1 : -1;
        };

        /**
         * Sort subspaces by their score in descending order.
         */
        public static final Comparator<Subspace> SORT_BY_SCORE_DESC = (o1, o2) -> {
            if(o1.score == o2.score) {
                return 0;
            }
            return o1.score < o2.score ? 1 : -1;
        };

    }

}
