package alexp.macrobase.pipeline.benchmark;

import alexp.macrobase.evaluation.*;
import alexp.macrobase.evaluation.chart.*;
import alexp.macrobase.evaluation.roc.Curve;
import alexp.macrobase.ingest.StreamingDataFrameLoader;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.normalization.Normalizer;
import alexp.macrobase.pipeline.Pipeline;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.utils.*;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import one.util.streamex.MoreCollectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClassifierEvaluationPipeline extends Pipeline {
    public static class RunResult {
        public final Curve curve;
        public final List<ResultPoint> points;
        public final int bestRank;

        public RunResult(Curve curve, List<ResultPoint> points, int bestRank) {
            this.curve = curve;
            this.points = points;
            this.bestRank = bestRank;
        }
    }

    private final PipelineConfig conf;

    private final Uri inputURI;

    private final String inputDirRelativePath; // for dir URI (csv://somedir/), if it contains subdirs with input

    private String[] metricColumns;
    private String originalTimeColumn;
    private String timeColumn;
    private String timeFormat;

    private String labelColumn;
    private String labelsFile;

    private List<DataFrame> dataFrames = new ArrayList<>();
    private List<int[]> labelsLists = new ArrayList<>();

    private List<PipelineConfig> classifierConfigs;

    private Map<String, PipelineConfig> gridSearchClassifierConfigs;
    private String searchMeasure;

    private boolean isStreaming = false;

    private String nabOutputDir;

    private ClassifierEvaluationPipeline(PipelineConfig conf, String inputDirRelativePath) throws Exception {
        this.conf = conf;
        this.inputDirRelativePath = inputDirRelativePath;

        inputURI = new Uri(conf.get("inputURI"));

        classifierConfigs = ConfigUtils.getObjectsList(conf, "classifiers");

        //noinspection unchecked
        metricColumns = ((List<String>) conf.get("metricColumns")).toArray(new String[0]);

        labelColumn = conf.get("labelColumn", "is_anomaly");
        labelsFile = conf.get("nabLabelFile", null);

        timeColumn = conf.get("timeColumn");
        timeFormat = conf.get("timeFormat", null);
        originalTimeColumn = timeColumn;
        if (timeFormat != null) {
            timeColumn = "!parsed_" + timeColumn;
        }

        ConfigUtils.addToAllConfigs(classifierConfigs, "timeColumn", timeColumn);

        searchMeasure = conf.get("searchMeasure", "");

        PipelineConfig gsConf = ConfigUtils.getObj(conf, "gridsearch");
        if (gsConf != null) {
            List<PipelineConfig> configs = ConfigUtils.getObjectsList(gsConf, "classifiers");
            ConfigUtils.addToAllConfigs(configs, "timeColumn", timeColumn);

            gridSearchClassifierConfigs = configs.stream().collect(Collectors.toMap(it -> it.get("classifier"), it -> it));

            searchMeasure = gsConf.get("searchMeasure", "");
        }
    }

    public ClassifierEvaluationPipeline(PipelineConfig conf) throws Exception {
        this(conf, null);
    }

    public void setStreaming(boolean streaming) {
        isStreaming = streaming;
    }

    public void run() throws Exception {
        if (inputURI.isDir()) {
            runDir();
            return;
        }

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
                        .saveToPng(Paths.get(chartOutputDir(), fileNamePrefix() + "all_" + measureFileName + fileNameSuffix + ".png").toString());
            }
        }
    }

    private void runDir() throws Exception {
        for (String path : inputURI.getDirFiles(true)) {
            conf.getValues().put("inputURI", inputURI.getOriginalString() + path);

            String subdirPath = FilenameUtils.getPath(path);

            ClassifierEvaluationPipeline pipeline = new ClassifierEvaluationPipeline(conf, subdirPath);
            pipeline.setStreaming(isStreaming);
            pipeline.setOutputDir(getOutputDir() + "/" + subdirPath);
            pipeline.setNabOutputDir(nabOutputDir);
            pipeline.setOutputIncludesInliers(isOutputIncludesInliers());

            pipeline.run();

            System.out.println();
            System.out.println();
        }
    }

    public void runGridSearch() throws Exception {
        System.out.println(inputURI.getOriginalString());

        loadDara();

        for (PipelineConfig classifierConf : gridSearchClassifierConfigs != null ? gridSearchClassifierConfigs.values(): classifierConfigs) {
            System.out.println();
            System.out.println(Pipelines.getClassifier(classifierConf, metricColumns).getClass().getSimpleName());

            SortedMap<Double, Map<String, Object>> results  = runGridSearch(classifierConf);

            System.out.println(searchMeasure.toUpperCase());
            results.forEach((score, params) -> System.out.println(String.format("%.4f: %s", score, params)));
        }
    }

    private List<Curve> run(PipelineConfig classifierConf) throws Exception {
        String classifierType = classifierConf.get("classifier");

        if (gridSearchClassifierConfigs != null && gridSearchClassifierConfigs.containsKey(classifierType)) {
            SortedMap<Double, Map<String, Object>> gsResults = runGridSearch(gridSearchClassifierConfigs.get(classifierType));

            System.out.println();
            gsResults.forEach((score, params) -> printInfo(String.format("%.4f: %s", score, params)));

            classifierConf = ConfigUtils.merge(classifierConf, Iterables.getLast(gsResults.values()));
        }

        Classifier classifier = Pipelines.getClassifier(classifierConf, metricColumns);

        System.out.println();
        printInfo(classifier.getClass().getName());
        printInfo(classifierConf.getValues().entrySet().stream().filter(it -> !it.getKey().equals("classifier") && !it.getKey().endsWith("Column")).collect(Collectors.toSet()));

        List<Curve> curves = new ArrayList<>();
        List<ResultPoint> points = new ArrayList<>();
        List<RunResult> results = new ArrayList<>();

        if (isStreaming) {
            Stopwatch sw = Stopwatch.createStarted();

            for (int i = 0; i < dataFrames.size(); i++) {
                int num = i + 1;

                System.out.println();
                System.out.println("Part #" + num);

                DataFrame dataFrame = dataFrames.get(i);
                int[] labels = labelsLists.get(i);

                RunResult result = run(classifier, dataFrame, labels, classifierType, fileNamePrefix(), Integer.toString(num));
                results.add(result);
                curves.add(result.curve);
                points.addAll(result.points);
            }

            final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS);
            System.out.println();
            System.out.println(String.format("Total time elapsed: %d ms (%.2f sec)", totalMs, totalMs / 1000.0));
        } else {
            DataFrame dataFrame = dataFrames.get(0);
            int[] labels = labelsLists.get(0);

            RunResult result = run(classifier, dataFrame, labels, classifierType, fileNamePrefix(), "");
            results.add(result);
            curves.add(result.curve);
            points = result.points;
        }

        if (metricColumns.length == 1) {
            new AnomalyDataChart()
                    .setName(classifierType.toUpperCase() + ", " + inputURI.shortDisplayPath())
                    .createAnomaliesChart(points)
                    .saveToPng(Paths.get(chartOutputDir(), fileNamePrefix() + "data_" + classifierType + ".png").toString());

            if (curves.stream().anyMatch(c -> c.rankingSize() <= 3) && results.stream().anyMatch(it -> it.bestRank == 0 || it.bestRank >= it.curve.rankingSize() - 1)) {
                double threshold = 0.5;
                for (ResultPoint point : points) {
                    point.setThreshold(threshold);
                }
                new AnomalyDataChart()
                        .setName(classifierType.toUpperCase() + ", " + inputURI.shortDisplayPath())
                        .createAnomaliesChart(points)
                        .saveToPng(Paths.get(chartOutputDir(), fileNamePrefix() + "data_" + classifierType + "_middle_threshold.png").toString());
            }
        }

        if (!StringUtils.isEmpty(getNabOutputDir())) {
            List<PipelineConfig> normalizersConfigs = ConfigUtils.getObjectsList(classifierConf, "normalizers");
            Normalizer normalizer = normalizersConfigs.isEmpty() ? null : Pipelines.getNormalizer(normalizersConfigs.get(0));

            saveNabOutput(Paths.get(getNabOutputDir(), classifierType, inputDirRelativePath).toString(), classifierType + "_" + inputURI.baseName(),
                    points, normalizer);
        }

        return curves;
    }

    private RunResult run(Classifier classifier, DataFrame dataFrame, int[] labels, String classifierType, String fileNamePrefix, String fileNameSuffix) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();

        classifier.process(dataFrame);

        final long classifierMs = sw.elapsed(TimeUnit.MILLISECONDS);
        printInfo(String.format("Time elapsed: %d ms (%.2f sec)", classifierMs, classifierMs / 1000.0));

        saveOutliers(fileNamePrefix + "outliers_" + classifierType + fileNameSuffix, classifier.getResults(), classifier.getOutputColumnName());

        double[] classifierResult = classifier.getResults().getDoubleColumnByName(classifier.getOutputColumnName());
        Curve aucAnalysis = aucCurve(classifierResult, labels);

        double rocArea = aucAnalysis.rocArea();
        double prArea = aucAnalysis.prArea();

        printInfo(String.format("ROC Area: %.4f", rocArea));
        printInfo(String.format("PR Area: %.4f", prArea));

        FScore fScore = new FScore();
        Pair<Integer, ConfusionMatrix> confusionMatrixIt = IntStream.range(0, aucAnalysis.rankingSize()).
                mapToObj(i -> new Pair<>(i, aucAnalysis.confusionMatrix(i)))
                .max(Comparator.comparing(it -> {
                    ConfusionMatrix matr = it.getValue();
                    double score = fScore.evaluate(matr);
                    return Double.isNaN(score) ? -1 : score;
                })).get();
        int rank = confusionMatrixIt.getKey();
        ConfusionMatrix confusionMatrix = confusionMatrixIt.getValue();
        double threshold = aucAnalysis.threshold(rank, classifierResult);

        printInfo(String.format("Stats for threshold (score > %.2f, rank %d) with the highest F1-score:", threshold, rank));
        printInfo(confusionMatrix);
        printInfo(String.format("Accuracy: %.4f", new Accuracy().evaluate(confusionMatrix)));
        printInfo(String.format("F1-score: %.4f", fScore.evaluate(confusionMatrix)));

        saveInfo(fileNamePrefix + classifierType + "_info" + fileNameSuffix);

        new AucChart()
                .setName(classifierType.toUpperCase() + ", " + inputURI.shortDisplayPath())
                .createForSingle(aucAnalysis,
                        AucChart.Measures.RocAuc, AucChart.Measures.PrAuc, AucChart.Measures.F1, AucChart.Measures.Accuracy)
                .saveToPng(Paths.get(chartOutputDir(), fileNamePrefix + classifierType + fileNameSuffix + ".png").toString());

        double[] time = dataFrame.getDoubleColumnByName(timeColumn);
        double[] values = dataFrame.getDoubleColumnByName(metricColumns[0]);
        String[] timeStrings = dataFrame.getSchema().getColumnTypeByName(originalTimeColumn) == Schema.ColType.STRING ? dataFrame.getStringColumnByName(originalTimeColumn) : null;
        List<ResultPoint> points = Streams.mapWithIndex(Arrays.stream(time), (t, ind) -> {
            int i = (int) ind;
            return new ResultPoint(t, timeStrings == null ? null : timeStrings[i], values[i], classifierResult[i], threshold, labels[i] == 1);
        }).collect(Collectors.toList());

        return new RunResult(aucAnalysis, points, rank);
    }

    private SortedMap<Double, Map<String, Object>> runGridSearch(PipelineConfig classifierConf) throws Exception {
        DataFrame dataFrame = dataFrames.get(0);
        int[] labels = labelsLists.get(0);

        Map<String, Object[]> searchParams = classifierConf.<ArrayList<Map<String, Object>>>get("searchParams").stream()
                .collect(Collectors.toMap(o -> o.keySet().iterator().next(), o -> ((ArrayList) o.values().iterator().next()).toArray()));

        GridSearch gs = new GridSearch();
        searchParams.forEach(gs::addParam);

        List<PipelineConfig> normalizersConfigs = ConfigUtils.getObjectsList(classifierConf, "normalizers");
        Normalizer normalizer = normalizersConfigs.isEmpty() ? null : Pipelines.getNormalizer(normalizersConfigs.get(0));

        OptionalDouble fixedThreshold = ConfigUtils.getOptionalDouble(classifierConf,"threshold");

        gs.run(params -> {
            PipelineConfig conf = ConfigUtils.merge(classifierConf, params);

            Classifier classifier = Pipelines.getClassifier(conf, metricColumns);

            classifier.process(dataFrame);

            DataFrame classifierResultDf = classifier.getResults();
            if (normalizer != null) {
                normalizer.setColumnName(classifier.getOutputColumnName()).setOutputColumnName(classifier.getOutputColumnName());
                normalizer.process(classifierResultDf);
                classifierResultDf = normalizer.getResults();
            }
            double[] classifierResult = classifierResultDf.getDoubleColumnByName(classifier.getOutputColumnName());

            switch (searchMeasure) {
                case "roc": return aucCurve(classifierResult, labels).rocArea();
                case "pr": return aucCurve(classifierResult, labels).prArea();
                case "f1": {
                    Curve curve = aucCurve(classifierResult, labels);
                    FScore fScore = new FScore();
                    return IntStream.range(0, curve.rocPoints().length)
                            .mapToDouble(i -> {
                                ConfusionMatrix matr = curve.confusionMatrix(i);
                                return matr.positiveCount() == 0 || matr.negativeCount() == 0 ? -1 : fScore.evaluate(matr);
                            })
                            .filter(d -> !Double.isNaN(d))
                            .max().getAsDouble();
                }
                case "nab": {
                    NabScore scorer = new NabScore();
                    if (fixedThreshold.isPresent()) {
                        boolean[] results = Arrays.stream(classifierResult)
                                .mapToObj(value -> fixedThreshold.getAsDouble() < value)
                                .collect(MoreCollectors.toBooleanArray(obj -> (boolean)obj));

                        return scorer.evaluate(results, labels);
                    }

                    Curve curve = aucCurve(classifierResult, labels);
                    return IntStream.range(0, curve.rankingSize())
                            .mapToObj(i -> new Pair<>(i, curve.confusionMatrix(i)))
                            .mapToDouble(it -> {
                                if (it.getValue().positiveCount() == 0 && curve.rankingSize() == 3) {
                                    return -99999999;
                                }
                                double threshold = curve.threshold(it.getKey(), classifierResult);
                                boolean[] results = Arrays.stream(classifierResult)
                                        .mapToObj(value -> threshold < value)
                                        .collect(MoreCollectors.toBooleanArray(obj -> (boolean)obj));

                                return scorer.evaluate(results, labels);
                            })
                            .max().getAsDouble();
                }
                default: throw new RuntimeException("Unknown search measure " + searchMeasure);
            }
        });

        return gs.getResults();
    }

    private int[] getLabels(DataFrame dataFrame) {
        if (labelsFile != null) {
            try {
                String json = FileUtils.readFileToString(new File(labelsFile), Charset.defaultCharset());
                JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();

                String[] pathSegments = inputURI.getPath().split("(/)|(\\\\)");
                String key = pathSegments[pathSegments.length - 2] + "/" + pathSegments[pathSegments.length - 1];

                double[] time = dataFrame.getDoubleColumnByName(timeColumn);
                double[] labels = new double[time.length];
                Arrays.fill(labels, 0);

                JsonArray windows = jsonObject.getAsJsonArray(key);
                for (JsonElement window : windows) {
                    JsonArray arr = window.getAsJsonArray();
                    long start = TimeUtils.dateTimeToUnixTimestamp(arr.get(0).getAsString(), timeFormat);
                    long end = TimeUtils.dateTimeToUnixTimestamp(arr.get(1).getAsString(), timeFormat);

                    for (int i = 0; i < labels.length; i++) {
                        if (time[i] >= start && time[i] <= end) {
                            labels[i] = 1.0;
                        }

                    }
                }
                dataFrame.addColumn(labelColumn, labels);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

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

                if (timeFormat != null) {
                    Pipelines.parseTimeColumn(result, originalTimeColumn, timeColumn, timeFormat);
                }
            });
        } else {
            dataFrames.add(loadBatchDara());
        }

        labelsLists = dataFrames.stream().map(this::getLabels).collect(Collectors.toList());
    }

    private DataFrame loadBatchDara() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();

        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());

        DataFrame dataFrame = Pipelines.loadDataFrame(inputURI, colTypes, requiredColumns, conf);

        createAutoGeneratedColumns(dataFrame, timeColumn);

        if (timeFormat != null) {
            Pipelines.parseTimeColumn(dataFrame, originalTimeColumn, timeColumn, timeFormat);
        }

        return dataFrame;
    }

    private StreamingDataFrameLoader getDataLoader() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();

        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());

        return Pipelines.getStreamingDataLoader(inputURI, colTypes, requiredColumns, conf);
    }

    private Map<String, Schema.ColType> getColTypes() {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(originalTimeColumn, timeFormat == null ? Schema.ColType.DOUBLE : Schema.ColType.STRING);
        if (labelsFile == null) {
            colTypes.put(labelColumn, Schema.ColType.DOUBLE);
        }
        for (String metricColumn : metricColumns) {
            colTypes.put(metricColumn, Schema.ColType.DOUBLE);
        }
        return colTypes;
    }

    private  void saveNabOutput(String dir, String name, List<ResultPoint> points, Normalizer normalizer) throws Exception {
        String[] timestamps = points.stream().map(ResultPoint::getTimeStr).toArray(String[]::new);
        double[] values = points.stream().mapToDouble(ResultPoint::getValue).toArray();
        double[] scores = points.stream().mapToDouble(ResultPoint::getScore).toArray();
        double[] labels = points.stream().mapToDouble(p -> p.isLabel() ? 1.0 : 0.0).toArray();

        DataFrame df = new DataFrame();
        df.addColumn("timestamp", timestamps);
        df.addColumn("value", values);
        df.addColumn(normalizer == null ? "anomaly_score" : "raw_score", scores);
        df.addColumn("label", labels);

        if (normalizer != null) {
            normalizer.setColumnName("raw_score").setOutputColumnName("anomaly_score");
            normalizer.process(df);
            df = normalizer.getResults();
        }

        saveData(dir, name, df);
    }

    public static String defaultOutputDir() {
        return "alexp/bench_output";
    }

    @Override
    public String getOutputDir() {
        return StringUtils.isEmpty(super.getOutputDir()) ? defaultOutputDir() :super. getOutputDir();
    }

    public String getNabOutputDir() {
        return nabOutputDir;
    }

    public void setNabOutputDir(String nabOutputDir) {
        this.nabOutputDir = nabOutputDir;
    }

    private String chartOutputDir() {
        return getOutputDir();
    }

    private String fileNamePrefix() {
        String prefix = inputURI.baseName();
        if (!prefix.isEmpty()) {
            prefix += "___";
        }
        return prefix;
    }
}
