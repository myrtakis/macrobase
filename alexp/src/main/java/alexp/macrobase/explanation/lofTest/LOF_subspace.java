package alexp.macrobase.explanation.lofTest;

import alexp.macrobase.ingest.Uri;
import alexp.macrobase.outlier.lof.bkaluza.LOF;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import alexp.macrobase.pipeline.benchmark.config.DatasetConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import javafx.util.Pair;
import org.omg.CORBA.INTERNAL;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LOF_subspace {

    private static String scoreColumn = "_OUTLIER";

    private static String datasetPath = "alexp/data/explanation/TestLOF/synth_010_000_2_3_4_5.yaml";

    private static List<Pair<Integer, String>> runClassifier(DataFrame df, String[] columns, AlgorithmConfig conf) throws Exception {
        Classifier classifier = Pipelines.getClassifier(conf.getAlgorithmId(), conf.getParameters(), columns, null);
        return getPointsScoresInRelSubspaces(df, Arrays.asList(columns), classifier);
    }

    private static List<Pair<Integer, String>> getPointsScoresInRelSubspaces(DataFrame df, List<String> relSubspace, Classifier classifier) throws Exception {
        List<Pair<Integer, String>> pointsRelSubspaces = new ArrayList<>();
        classifier.process(df);
        DataFrame results = classifier.getResults();
        List<Pair<Integer, Double>> pointsScores = new ArrayList<>();
        for(int i=0; i < results.getNumRows(); i++) {
            pointsScores.add(new Pair<>(i, results.getRow(i).getAs(scoreColumn)));
        }
        pointsScores.sort(Comparator.comparing(Pair::getValue));
        Collections.reverse(pointsScores);
        for(Pair<Integer, Double> pair : pointsScores){
            String relSubspaceStr = subspaceToString(relSubspace, pair.getValue());
            pointsRelSubspaces.add(new Pair<>(pair.getKey(), relSubspaceStr));
        }
        return pointsRelSubspaces;
    }

    private static String subspaceToString(List<String> subspaceFeatures, double score) {
        return "[" + Joiner.on(" ").join(subspaceFeatures) + "] " + score + ";";
    }

    private static DataFrame loadDataFrame(String datasetFilePath, String[] features) throws Exception {
        Map<String, Schema.ColType> colTypes = Arrays.stream(features)
                .collect(Collectors.toMap(Function.identity(), c -> Schema.ColType.DOUBLE));

        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());

        return Pipelines.loadDataFrame(new Uri("csv://"+datasetFilePath), colTypes, requiredColumns, null);
    }

    private static void validator(HashMap<Integer, String> pointsRelSubspaces) {
        System.out.println("Print points accompanied by their subspace and score in this subspace");
        for(int pointId : pointsRelSubspaces.keySet()) {
            System.out.println(pointId + " -> " + pointsRelSubspaces.get(pointId));
        }

//        System.out.println("\nInliers scored higher than outliers");
//        pointsRelSubspaces.keySet().removeAll(pointsOfInterest);
//
    }

    private static void test_subspace_2_3_4_5(AlgorithmConfig classifierConf, DatasetConfig datasetConfig, int elementsToKeep) throws Exception{

        HashSet<Integer> pointsOfInterest = Sets.newHashSet(172, 183, 184, 207, 220, 245, 315, 323, 477, 510, 577, 654, 704, 723, 754, 765, 781, 824, 975);

        DataFrame df = loadDataFrame(datasetConfig.getDatasetId(), datasetConfig.getMetricColumns());

        List<Pair<Integer, String>> pointsRelSubspaces = runClassifier(df, datasetConfig.getMetricColumns(), classifierConf);

        System.out.println("\nPoints of interest\n");
        System.out.println(pointsOfInterest);

        System.out.println("\nPoints with top-" + elementsToKeep + " scores accompanied by their subspace and score in this subspace\n");
        for(int i =0; i < elementsToKeep; i++) {
            System.out.println(pointsRelSubspaces.get(i));
        }

        List<Integer> topK = pointsRelSubspaces.subList(0, elementsToKeep)
                .stream()
                .map(Pair::getKey)
                .limit(elementsToKeep)
                .collect(Collectors.toList());

        System.out.println("\nInliers scored higher than outliers\n");
        HashSet<Integer> inliersInTopK = new HashSet<>(topK);
        inliersInTopK.removeAll(pointsOfInterest);
        System.out.println(inliersInTopK);
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig bc = BenchmarkConfig.load(StringObjectMap.fromYamlFile(datasetPath));
        for(AlgorithmConfig classifierConf : bc.getClassifierConfigs())
            test_subspace_2_3_4_5(classifierConf, bc.getDatasetConfig(), 5);
    }



}
