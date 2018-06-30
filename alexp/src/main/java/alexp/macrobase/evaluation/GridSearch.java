package alexp.macrobase.evaluation;

import alexp.macrobase.evaluation.roc.Curve;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.outlier.MinCovDet;
import alexp.macrobase.pipeline.Pipelines;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.util.*;

public class GridSearch {

    @FunctionalInterface
    public interface RunInstance {
        double accept(double[] params) throws Exception;
    }

    public void run(List<double[]> paramsLists, RunInstance runInstance) throws Exception {
        List<double[]> permutations = new ArrayList<>();
        generatePermutations(paramsLists, permutations, 0, new ArrayList<>());

        Map<Double, double[]> results = new TreeMap<>();

        for (double[] permutation : permutations) {
            results.put(runInstance.accept(permutation), permutation);
        }

        results.forEach((k, p) -> System.out.println(String.format("%s - %.4f", Arrays.toString(p), k)));
    }

    private void generatePermutations(List<double[]> paramsLists, List<double[]> result, int depth, List<Double> current)
    {
        if(depth == paramsLists.size())
        {
            result.add(current.stream().mapToDouble(i->i).toArray());
            return;
        }

        for(int i = 0; i < paramsLists.get(depth).length; ++i)
        {
            List<Double> next = new ArrayList<>(current);
            next.add(paramsLists.get(depth)[i]);
            generatePermutations(paramsLists, result, depth + 1, next);
        }
    }

    public static void main(String[] args) throws Exception {
        String[] metrics = new String[] { "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9" };
        String labelColumn = "is_anomaly";
        DataFrame dataFrame = loadDara("csv://alexp/data/outlier/shuttle-unsupervised-ad.csv", metrics, labelColumn);
        int[] labels = Arrays.stream(dataFrame.getDoubleColumnByName(labelColumn)).mapToInt(d -> (int) d).toArray();

        ArrayList<double[]> params = new ArrayList<>();
        params.add(new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 });
        params.add(new double[] { 0.1, 0.01, 0.001, 0.0001 });

        GridSearch gs = new GridSearch();
        gs.run(params, p -> {
            MinCovDet classifier = new MinCovDet(metrics);
            classifier.setAlpha(p[0]);
            classifier.setStoppingDelta(p[1]);

            classifier.process(dataFrame);

            double[] classifierResult = classifier.getResults().getDoubleColumnByName(classifier.getOutputColumnName());
            Curve aucAnalysis = new Curve.PrimitivesBuilder()
                    .scores(classifierResult)
                    .labels(labels)
                    .build();

            return aucAnalysis.rocArea();
        });
    }

    private static DataFrame loadDara(String url, String[] metrics, String labelColumn) throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(labelColumn, Schema.ColType.DOUBLE);
        for (String metricColumn : metrics) {
            colTypes.put(metricColumn, Schema.ColType.DOUBLE);
        }

        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());

        return Pipelines.loadDataFrame(new Uri(url), colTypes, requiredColumns, null);
    }
}
