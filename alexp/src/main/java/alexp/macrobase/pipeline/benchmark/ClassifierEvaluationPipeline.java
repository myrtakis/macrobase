package alexp.macrobase.pipeline.benchmark;

import alexp.macrobase.evaluation.*;
import alexp.macrobase.evaluation.roc.Curve;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.ingest.XlsxDataFrameReader;
import alexp.macrobase.outlier.MAD;
import alexp.macrobase.outlier.mcod.McodClassifier;
import com.google.common.base.Stopwatch;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClassifierEvaluationPipeline {
    private final Uri inputURI;

    private String metric;
    private String timeColumn;
    private String labelColumn;

    private List<Classifier> classifiers;

    private DataFrame dataFrame;
    private int[] labels;

    public ClassifierEvaluationPipeline(PipelineConfig conf) throws Exception {
        inputURI = new Uri(conf.get("inputURI"));

        timeColumn = conf.get("timeColumn", "id");
        metric = conf.get("metricColumn", "value");
        labelColumn = conf.get("labelColumn", "is_anomaly");

        ArrayList<Map<String, Object>> classifierConfigs = conf.get("classifiers");
        classifiers = classifierConfigs.stream().map(this::getClassifier).collect(Collectors.toList());

        dataFrame = loadDara();
        labels = Arrays.stream(dataFrame.getDoubleColumnByName(labelColumn)).mapToInt(d -> (int) d).toArray();
    }

    public void run() throws Exception {
        for (Classifier classifier : classifiers) {
            run(classifier);
        }
    }

    private void run(Classifier classifier) throws Exception {
        System.out.println();
        System.out.println(classifier.getClass().getSimpleName());

        Stopwatch sw = Stopwatch.createStarted();

        classifier.process(dataFrame);

        final long classifierMs = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Time elapsed: %d ms (%.2f sec)", classifierMs, classifierMs / 1000.0));

        double[] classifierResult = classifier.getResults().getDoubleColumnByName(classifier.getOutputColumnName());
        Curve aucAnalysis = new Curve.PrimitivesBuilder()
                .scores(classifierResult)
                .labels(labels)
                .build();

        double rocArea = aucAnalysis.rocArea();
        double prArea = aucAnalysis.prArea();

        System.out.println(String.format("ROC Area: %.4f", rocArea));
        System.out.println(String.format("PR Area: %.4f", prArea));

        System.out.println("Stats for middle threshold:");

        int middleRank = aucAnalysis.rocPoints().length / 2;
        ConfusionMatrix confusionMatrix = aucAnalysis.confusionMatrix(middleRank);

        System.out.println(confusionMatrix);

        System.out.println(String.format("Accuracy: %.4f", new Accuracy().evaluate(confusionMatrix)));
        System.out.println(String.format("F1-score: %.4f", new FScore().evaluate(confusionMatrix)));
    }

    private DataFrame loadDara() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(timeColumn, Schema.ColType.DOUBLE);
        colTypes.put(metric, Schema.ColType.DOUBLE);
        colTypes.put(labelColumn, Schema.ColType.DOUBLE);

        List<String> requiredColumns = new ArrayList<>();
        requiredColumns.add(timeColumn);
        requiredColumns.add(metric);
        requiredColumns.add(labelColumn);

        switch (inputURI.getType()) {
            case XLSX:
                return new XlsxDataFrameReader(inputURI.getPath(), requiredColumns, 0).load();
            default:
                return PipelineUtils.loadDataFrame(inputURI.getOriginalString(), colTypes, requiredColumns);
        }
    }

    private Classifier getClassifier(Map<String, Object> conf) throws RuntimeException {
        String classifierType = (String) conf.get("classifier");

        switch (classifierType.toLowerCase()) {
            case "mcod": {
                McodClassifier classifier = new McodClassifier(metric);
                classifier.setMaxDistance((double) conf.getOrDefault("maxDistance", 1.0));
                classifier.setMinNeighborCount((int) conf.getOrDefault("minNeighborCount", 30));
                classifier.setWindowSize((int) conf.getOrDefault("classifierWindowSize", 9999));
                classifier.setSlide((int) conf.getOrDefault("classifierSlide", 9999));
                classifier.setTimeColumnName(timeColumn);
                return classifier;
            }
            case "percentile": {
                PercentileClassifier classifier = new PercentileClassifier(metric);
                classifier.setPercentile((double) conf.getOrDefault("cutoff", 1.0));
                classifier.setIncludeHigh((boolean) conf.getOrDefault("includeHi",true));
                classifier.setIncludeLow((boolean) conf.getOrDefault("includeLo",true));
                return classifier;
            }
            case "mad": {
                return new MAD(metric);
            }
            default : {
                throw new RuntimeException("Bad Classifier Type");
            }
        }
    }
}
