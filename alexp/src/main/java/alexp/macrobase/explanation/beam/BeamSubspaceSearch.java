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
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

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
     * The beam width.
     */
    private int W;

    /**
     * The output DataFrame.
     */
    private DataFrame output;

    public BeamSubspaceSearch(String[] columns, AlgorithmConfig classifierConf, String datasetPath, ExplanationSettings explanationSettings) {
        super(columns, classifierConf, datasetPath, explanationSettings);
    }

    @Override
    public void process(DataFrame input) throws Exception {
        output = input.copy();
        HashMap<Integer, List<Subspace>> pointsSubspaces =  calculateSubspaces(input);
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

    private HashMap<Integer, List<Subspace>> calculateSubspaces(DataFrame input) throws Exception {
        HashMap<Integer, List<Subspace>> pointsScores = new HashMap<>();

        HashMap<Integer, TopBoundedHeap<Subspace>> points2dScores = score2dSubspaces(input);

        for(int pointId : points2dScores.keySet()) {
            List<Subspace> topSubspaces;
            if(dmax > 2){
                System.out.println("Scoring point " + pointId);
                topSubspaces = scorePointInMultiDimSubspaces(input, points2dScores.get(pointId), pointId);
                topSubspaces.sort(Subspace.SORT_BY_SCORE_DESC);
            } else {
                topSubspaces = points2dScores.get(pointId).topK(topk, Subspace.SORT_BY_SCORE_DESC);
            }
            pointsScores.put(pointId, topSubspaces);
        }
        return pointsScores;
    }

    private List<Subspace> scorePointInMultiDimSubspaces(DataFrame input, TopBoundedHeap<Subspace> best2dSubspaces,
                                                         int pointId) throws Exception {
        TopBoundedHeap<Subspace> globalList = new TopBoundedHeap<>(topk, Subspace.SORT_BY_SCORE_ASC);
        TopBoundedHeap<Subspace> stageList = new TopBoundedHeap<>(best2dSubspaces);
        HashSet<String> consideredFeatures = new HashSet<>();
        for(int i = 3; i <= dmax; i++){
            TopBoundedHeap<Subspace> candidates = new TopBoundedHeap<>(stageList);  // copy the previous stage best subspaces heap to the candidates heap
            stageList.clear();
            consideredFeatures.clear();
            for(Heap<Subspace>.UnorderedIter it =  candidates.unorderedIter(); it.valid(); it.advance()){
                for(int featureId = 0; featureId < getDatasetDimensionality(); featureId++){
                    Subspace newSubspace = new Subspace(it.get());  // copy previous subspace to the new one
                    int oldSubspaceDim = newSubspace.getDimensionality();
                    newSubspace.addFeature(featureId);
                    if(newSubspace.getDimensionality() == oldSubspaceDim || consideredFeatures.contains(newSubspace.getFeatures().toString()))
                        continue;
                    System.out.print("\rStage " + i + ": " + newSubspace.getFeatures());
                    DataFrame results = runClassifier(input, newSubspace);
                    newSubspace.setScore(results.getDoubleColumnByName(outputColumnName)[pointId]);
                    consideredFeatures.add(newSubspace.getFeatures().toString());
                    globalList.add(newSubspace);
                    stageList.add(newSubspace);
                }
            }
            System.out.println();
        } // end for
        return globalList.toList();
    }

    private HashMap<Integer, TopBoundedHeap<Subspace>> score2dSubspaces(DataFrame input) throws Exception {
        System.out.println("Calculating scores in 2-dimensional projections exhaustively...");
        HashMap<Integer, TopBoundedHeap<Subspace>> pointsScores = new HashMap<>();
        int counter = 0;
        int totalSubspaces = binomialCoeff(columns.length, 2);
        for(int i = 0; i < getDatasetDimensionality(); i++) {
            for(int j = i + 1; j < getDatasetDimensionality(); j++) {
                System.out.print("\rSubspace " + (++counter) + "/" + totalSubspaces);
                Subspace subspace = new Subspace(Sets.newHashSet(i, j));
                DataFrame results = runClassifier(input, subspace);
                double[] scores = results.getDoubleColumnByName(outputColumnName);
                for(int pointId : getPointsToExplain()) {
                    Subspace pointSubspace = new Subspace(subspace.getFeatures(), scores[pointId]);
                    TopBoundedHeap<Subspace> pointTopWSubspaces = pointsScores.getOrDefault(pointId, new TopBoundedHeap<>(W, Subspace.SORT_BY_SCORE_ASC));
                    pointTopWSubspaces.add(pointSubspace);
                    pointsScores.put(pointId, pointTopWSubspaces);
                }
            }
        }
        System.out.println();
        return pointsScores;
    }

    private int binomialCoeff(int n, int k)
    {
        // Base Cases
        if (k == 0 || k == n)
            return 1;

        // Recur
        return binomialCoeff(n - 1, k - 1) +
                binomialCoeff(n - 1, k);
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

}
