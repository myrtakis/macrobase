package alexp.macrobase.pipeline.benchmark;

import alexp.macrobase.evaluation.*;
import alexp.macrobase.evaluation.roc.Curve;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.pipeline.Pipeline;
import alexp.macrobase.pipeline.Pipelines;
import com.google.common.base.Stopwatch;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClassifierEvaluationPipeline extends Pipeline {
    private final PipelineConfig conf;

    private final Uri inputURI;

    private String[] metricColumns;
    private String timeColumn;
    private String labelColumn;

    private ArrayList<Map<String, Object>> classifierConfigs;

    private DataFrame dataFrame;
    private int[] labels;

    private final String searchMeasure;

    public ClassifierEvaluationPipeline(PipelineConfig conf) throws Exception {
        this.conf = conf;

        inputURI = new Uri(conf.get("inputURI"));

        //noinspection unchecked
        metricColumns = ((List<String>) conf.get("metricColumns")).toArray(new String[0]);

        labelColumn = conf.get("labelColumn", "is_anomaly");
        timeColumn = conf.get("timeColumn");

        classifierConfigs = conf.get("classifiers");
        classifierConfigs.forEach(c -> {
            c.put("timeColumn", timeColumn);
        });

        searchMeasure = conf.get("searchMeasure", "");

        dataFrame = loadDara();
        labels = Arrays.stream(dataFrame.getDoubleColumnByName(labelColumn)).mapToInt(d -> (int) d).toArray();
    }

    public void run() throws Exception {
        System.out.println(inputURI.getOriginalString());

        for (Map<String, Object> classifierConf : classifierConfigs) {
            run(new PipelineConfig(classifierConf));
        }
    }

    public void runGridSearch() throws Exception {
        System.out.println(inputURI.getOriginalString());

        for (Map<String, Object> classifierConf : classifierConfigs) {
            runGridSearch(new PipelineConfig(classifierConf));
        }
    }

    private void run(PipelineConfig classifierConf) throws Exception {
        Classifier classifier = Pipelines.getClassifier(classifierConf, metricColumns);

        String classifierType = classifierConf.get("classifier");

        System.out.println();
        System.out.println(classifier.getClass().getName());
        System.out.println(classifierConf.getValues().entrySet().stream().filter(it -> !it.getKey().equals("classifier") && !it.getKey().endsWith("Column")).collect(Collectors.toSet()));

        Stopwatch sw = Stopwatch.createStarted();

        classifier.process(dataFrame);

        final long classifierMs = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Time elapsed: %d ms (%.2f sec)", classifierMs, classifierMs / 1000.0));

        saveOutliers("outliers_" + classifierType, classifier);

        double[] classifierResult = classifier.getResults().getDoubleColumnByName(classifier.getOutputColumnName());
        Curve aucAnalysis = aucCurve(classifierResult);

        double rocArea = aucAnalysis.rocArea();
        double prArea = aucAnalysis.prArea();

        System.out.println(String.format("ROC Area: %.4f", rocArea));
        System.out.println(String.format("PR Area: %.4f", prArea));

        System.out.println("Stats for threshold with the highest F1-score:");

        FScore fScore = new FScore();
        ConfusionMatrix confusionMatrix = IntStream.range(0, aucAnalysis.rankingSize()).
                mapToObj(aucAnalysis::confusionMatrix)
                .max(Comparator.comparing(matr -> {
                    double score = fScore.evaluate(matr);
                    return Double.isNaN(score) ? -1 : score;
                })).get();

        System.out.println(confusionMatrix);
        System.out.println(String.format("Accuracy: %.4f", new Accuracy().evaluate(confusionMatrix)));
        System.out.println(String.format("F1-score: %.4f", fScore.evaluate(confusionMatrix)));

        new AucChart()
                .setName(classifierType.toUpperCase() + ", " + inputURI.shortDisplayPath())
                .saveToPng(aucAnalysis, Paths.get(chartOutputDir(), classifierType + ".png").toString());
    }

    private void runGridSearch(PipelineConfig classifierConf) throws Exception {
        System.out.println();
        System.out.println(Pipelines.getClassifier(classifierConf, metricColumns).getClass().getSimpleName());

        Map<String, Object[]> searchParams = classifierConf.<ArrayList<Map<String, Object>>>get("searchParams").stream()
                .collect(Collectors.toMap(o -> o.keySet().iterator().next(), o -> ((ArrayList) o.values().iterator().next()).toArray()));

        GridSearch gs = new GridSearch();
        searchParams.forEach(gs::addParam);

        gs.run(params -> {
            Map<String, Object> currConf = new HashMap<>(classifierConf.getValues());
            currConf.putAll(params);

            Classifier classifier = Pipelines.getClassifier(new PipelineConfig(currConf), metricColumns);

            classifier.process(dataFrame);

            double[] classifierResult = classifier.getResults().getDoubleColumnByName(classifier.getOutputColumnName());

            switch (searchMeasure) {
                case "roc": return aucCurve(classifierResult).rocArea();
                case "pr": return aucCurve(classifierResult).prArea();
                case "f1": {
                    Curve curve = aucCurve(classifierResult);
                    FScore fScore = new FScore();
                    return IntStream.range(0, curve.rocPoints().length)
                            .mapToDouble(i -> fScore.evaluate(curve.confusionMatrix(i)))
                            .filter(d -> !Double.isNaN(d))
                            .max().getAsDouble();
                }
                default: throw new RuntimeException("Unknown search measure " + searchMeasure);
            }
        });

        System.out.println(searchMeasure.toUpperCase());
        gs.getResults().forEach((score, params) -> System.out.println(String.format("%.4f: %s", score, params)));
    }

    private Curve aucCurve(double[] classifierResult) {
        return new Curve.PrimitivesBuilder()
                .scores(classifierResult)
                .labels(labels)
                .build();
    }

    private DataFrame loadDara() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(timeColumn, Schema.ColType.DOUBLE);
        colTypes.put(labelColumn, Schema.ColType.DOUBLE);
        for (String metricColumn : metricColumns) {
            colTypes.put(metricColumn, Schema.ColType.DOUBLE);
        }

        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());

        DataFrame dataFrame = Pipelines.loadDataFrame(inputURI, colTypes, requiredColumns, conf);

        createAutoGeneratedColumns(dataFrame, timeColumn);

        return dataFrame;
    }

    private String chartOutputDir() {
        return StringUtils.isEmpty(getOutputDir()) ? "alexp/bench_output" : getOutputDir();
    }
}
