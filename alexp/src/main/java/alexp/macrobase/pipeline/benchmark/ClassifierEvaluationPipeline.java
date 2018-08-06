package alexp.macrobase.pipeline.benchmark;

import alexp.macrobase.evaluation.*;
import alexp.macrobase.evaluation.roc.Curve;
import alexp.macrobase.ingest.StreamingDataFrameLoader;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.pipeline.Pipeline;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.utils.CollectionUtils;
import alexp.macrobase.utils.ConfigUtils;
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

    private List<DataFrame> dataFrames = new ArrayList<>();
    private List<int[]> labelsLists = new ArrayList<>();

    private List<PipelineConfig> classifierConfigs;

    private final String searchMeasure;

    private boolean isStreaming = false;

    public ClassifierEvaluationPipeline(PipelineConfig conf) throws Exception {
        this.conf = conf;

        inputURI = new Uri(conf.get("inputURI"));

        classifierConfigs = ConfigUtils.getObjectsList(conf, "classifiers");

        //noinspection unchecked
        metricColumns = ((List<String>) conf.get("metricColumns")).toArray(new String[0]);

        labelColumn = conf.get("labelColumn", "is_anomaly");
        timeColumn = conf.get("timeColumn");

        ConfigUtils.addToAllConfigs(classifierConfigs, "timeColumn", timeColumn);

        searchMeasure = conf.get("searchMeasure", "");
    }

    public void setStreaming(boolean streaming) {
        isStreaming = streaming;
    }

    public void run() throws Exception {
        System.out.println(inputURI.getOriginalString());

        loadDara();

        List<List<Curve>> aucCurves = new ArrayList<>();

        for (PipelineConfig classifierConf : classifierConfigs) {
            List<Curve> classifierCurves = run(classifierConf);
            aucCurves.add(classifierCurves);
        }

        aucCurves = CollectionUtils.transpose(aucCurves);

        List<String> classifierNames = classifierConfigs.stream().map(c -> c.<String>get("classifier").toUpperCase()).collect(Collectors.toList());
        List<AucChart.Measure> measures = Arrays.asList(AucChart.Measures.RocAuc, AucChart.Measures.PrAuc, AucChart.Measures.F1, AucChart.Measures.Accuracy);

        for (int i = 0; i < aucCurves.size(); i++) {
            List<Curve> classifierCurves = aucCurves.get(i);

            String fileNameSuffix = isStreaming ? Integer.toString(i + 1) : "";

            for (AucChart.Measure measure : measures) {
                String measureFileName = measure.name.toLowerCase().replace(" ", "_").replace("-", "_");

                new AucChart()
                        .setName(measure.name + ", " + inputURI.shortDisplayPath())
                        .createForAll(classifierCurves, classifierNames, measure)
                        .saveToPng(Paths.get(chartOutputDir(), "all_" + measureFileName + fileNameSuffix + ".png").toString());
            }
        }
    }

    public void runGridSearch() throws Exception {
        System.out.println(inputURI.getOriginalString());

        loadDara();

        for (PipelineConfig classifierConf : classifierConfigs) {
            runGridSearch(classifierConf);
        }
    }

    private List<Curve> run(PipelineConfig classifierConf) throws Exception {
        Classifier classifier = Pipelines.getClassifier(classifierConf, metricColumns);

        String classifierType = classifierConf.get("classifier");

        System.out.println();
        System.out.println(classifier.getClass().getName());
        System.out.println(classifierConf.getValues().entrySet().stream().filter(it -> !it.getKey().equals("classifier") && !it.getKey().endsWith("Column")).collect(Collectors.toSet()));

        List<Curve> curves = new ArrayList<>();

        if (isStreaming) {
            Stopwatch sw = Stopwatch.createStarted();

            for (int i = 0; i < dataFrames.size(); i++) {
                int num = i + 1;

                System.out.println();
                System.out.println("Part #" + num);

                DataFrame dataFrame = dataFrames.get(i);
                int[] labels = labelsLists.get(i);

                Curve curve = run(classifier, dataFrame, labels, classifierType, Integer.toString(num));
                curves.add(curve);
            }

            final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS);
            System.out.println();
            System.out.println(String.format("Total time elapsed: %d ms (%.2f sec)", totalMs, totalMs / 1000.0));
        } else {
            DataFrame dataFrame = dataFrames.get(0);
            int[] labels = labelsLists.get(0);

            Curve curve = run(classifier, dataFrame, labels, classifierType, "");
            curves.add(curve);
        }

        return curves;
    }

    private Curve run(Classifier classifier, DataFrame dataFrame, int[] labels, String classifierType, String fileNameSuffix) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();

        classifier.process(dataFrame);

        final long classifierMs = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Time elapsed: %d ms (%.2f sec)", classifierMs, classifierMs / 1000.0));

        saveOutliers("outliers_" + classifierType + fileNameSuffix, classifier.getResults(), classifier.getOutputColumnName());

        double[] classifierResult = classifier.getResults().getDoubleColumnByName(classifier.getOutputColumnName());
        Curve aucAnalysis = aucCurve(classifierResult, labels);

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
                .createForSingle(aucAnalysis,
                        AucChart.Measures.RocAuc, AucChart.Measures.PrAuc, AucChart.Measures.F1, AucChart.Measures.Accuracy)
                .saveToPng(Paths.get(chartOutputDir(), classifierType + fileNameSuffix + ".png").toString());

        return aucAnalysis;
    }

    private void runGridSearch(PipelineConfig classifierConf) throws Exception {
        DataFrame dataFrame = dataFrames.get(0);
        int[] labels = labelsLists.get(0);

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
                case "roc": return aucCurve(classifierResult, labels).rocArea();
                case "pr": return aucCurve(classifierResult, labels).prArea();
                case "f1": {
                    Curve curve = aucCurve(classifierResult, labels);
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

    private int[] getLabels(DataFrame dataFrame) {
        return Arrays.stream(dataFrame.getDoubleColumnByName(labelColumn)).mapToInt(d -> (int) d).toArray();
    }

    private Curve aucCurve(double[] classifierResult, int[] labels) {
        return new Curve.PrimitivesBuilder()
                .scores(classifierResult)
                .labels(labels)
                .build();
    }

    private void loadDara() throws Exception {
        dataFrames.clear();

        if (isStreaming) {
            StreamingDataFrameLoader dataLoader = getDataLoader();

            dataLoader.load(result -> {
                dataFrames.add(result);

                createAutoGeneratedColumns(result, timeColumn);
            });
        } else {
            dataFrames.add(loadBatchDara());
        }

        labelsLists = dataFrames.stream().map(this::getLabels).collect(Collectors.toList());
    }

    private DataFrame loadBatchDara() throws Exception {
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

    private StreamingDataFrameLoader getDataLoader() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(timeColumn, Schema.ColType.DOUBLE);
        colTypes.put(labelColumn, Schema.ColType.DOUBLE);
        for (String metricColumn : metricColumns) {
            colTypes.put(metricColumn, Schema.ColType.DOUBLE);
        }

        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());

        return Pipelines.getStreamingDataLoader(inputURI, colTypes, requiredColumns, conf);
    }

    private String chartOutputDir() {
        return StringUtils.isEmpty(getOutputDir()) ? "alexp/bench_output" : getOutputDir();
    }
}
