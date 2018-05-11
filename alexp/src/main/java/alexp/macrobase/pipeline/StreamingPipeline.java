package alexp.macrobase.pipeline;

import alexp.macrobase.ingest.HttpCsvStreamReader;
import alexp.macrobase.ingest.SqlStreamReader;
import alexp.macrobase.ingest.StreamingDataFrameLoader;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.outlier.mcod.McodClassifier;
import com.google.common.base.Stopwatch;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.operator.Operator;
import edu.stanford.futuredata.macrobase.operator.WindowedOperator;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class StreamingPipeline {
    private final Uri inputURI;

    private String idColumn;
    private String sqlQuery;

    private Integer maxReadBatchSize;

    private String classifierType;
    private String metric;
    private double cutoff;
    private String strCutoff;
    private boolean isStrPredicate;
    private boolean pctileHigh;
    private boolean pctileLow;
    private String predicateStr;
    private double maxDistance;
    private int minNeighborCount;
    private int classifierWindowLength;
    private int classifierSlideLength;

    private String summarizerType;
    private List<String> attributes;
    private double minSupport;

    private Integer numPanes;
    private Integer windowLength;
    private Integer slideLength;
    private String timeColumn;

    public StreamingPipeline(PipelineConfig conf) {
        inputURI = new Uri(conf.get("inputURI"));

        classifierType = conf.get("classifier", "percentile");
        metric = conf.get("metric");

        if (inputURI.getType() == Uri.Type.JDBC) {
            sqlQuery = conf.get("query");
            idColumn = conf.get("idColumn", "id");
        }

        maxReadBatchSize = conf.get("maxReadBatchSize", 5000);

        if (classifierType.equals("predicate")) {
            Object rawCutoff = conf.get("cutoff");
            isStrPredicate = rawCutoff instanceof String;
            if (isStrPredicate) {
                strCutoff = (String) rawCutoff;
            } else {
                cutoff = (double) rawCutoff;
            }
        } else {
            isStrPredicate = false;
            cutoff = conf.get("cutoff", 1.0);
        }

        pctileHigh = conf.get("includeHi",true);
        pctileLow = conf.get("includeLo", true);
        predicateStr = conf.get("predicate", "==").trim();

        maxDistance = conf.get("maxDistance", 1.0);
        minNeighborCount = conf.get("minNeighborCount", 30);
        classifierWindowLength = conf.get("classifierWindowSize", 9999);
        classifierSlideLength = conf.get("classifierSlide", 9999);

        summarizerType = conf.get("summarizer", "apriori");
        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 0.01);

        numPanes = conf.get("numPanes", 3);
        windowLength = conf.get("windowLength", 6000);
        slideLength = conf.get("slideLength", 1000);
        timeColumn = conf.get("timeColumn", "time");
    }

    public void run(Consumer<Explanation> resultCallback) throws Exception {
        StreamingDataFrameLoader dataLoader = getDataLoader();

        Classifier classifier = getClassifier();
        Operator<DataFrame, ? extends Explanation> summarizer = getSummarizer(classifier.getOutputColumnName());

        AtomicLong totalClassifierMs = new AtomicLong();
        AtomicLong totalExplanationMs = new AtomicLong();

        dataLoader.load(dataFrame -> {
            Stopwatch sw = Stopwatch.createStarted();

            classifier.process(dataFrame);

            final long classifierMs = sw.elapsed(TimeUnit.MILLISECONDS);
            totalClassifierMs.addAndGet(classifierMs);
            sw = Stopwatch.createStarted();

            summarizer.process(classifier.getResults());

            final long explanationMs = sw.elapsed(TimeUnit.MILLISECONDS);
            totalExplanationMs.addAndGet(explanationMs);

            System.out.printf("Classification time: %d ms (total %d ms)\nSummarization time: %d ms (total %d ms)\n\n",
                    classifierMs, totalClassifierMs.get(), explanationMs, totalExplanationMs.get());

            resultCallback.accept(summarizer.getResults());
        });
    }

    private StreamingDataFrameLoader getDataLoader() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(metric);
        if (summarizerType.equals("windowed")) {
            requiredColumns.add(timeColumn);
        }

        switch (inputURI.getType()) {
            case HTTP:
                return new HttpCsvStreamReader(inputURI.getPath(), requiredColumns)
                        .setColumnTypes(colTypes);
            case JDBC:
                return new SqlStreamReader(inputURI.getPath(), requiredColumns, sqlQuery, idColumn)
                        .setColumnTypes(colTypes)
                        .setMaxBatchSize(maxReadBatchSize);
            default:
                throw new Exception("Unsupported input protocol");
        }
    }

    private Classifier getClassifier() throws MacroBaseException {
        switch (classifierType.toLowerCase()) {
            case "mcod": {
                McodClassifier classifier = new McodClassifier(metric);
                classifier.setMaxDistance(maxDistance);
                classifier.setMinNeighborCount(minNeighborCount);
                classifier.setWindowSize(classifierWindowLength);
                classifier.setSlide(classifierSlideLength);
                return classifier;
            }
            case "percentile": {
                PercentileClassifier classifier = new PercentileClassifier(metric);
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(pctileHigh);
                classifier.setIncludeLow(pctileLow);
                return classifier;
            }
            case "predicate": {
                if (isStrPredicate){
                    return new PredicateClassifier(metric, predicateStr, strCutoff);
                }
                return new PredicateClassifier(metric, predicateStr, cutoff);
            }
            default : {
                throw new MacroBaseException("Bad Classifier Type");
            }
        }
    }

    private Operator<DataFrame, ? extends Explanation> getSummarizer(String outlierColumnName) throws MacroBaseException {
        switch (summarizerType.toLowerCase()) {
            case "incremental": {
                IncrementalSummarizer summarizer = new IncrementalSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setWindowSize(numPanes);
                return summarizer;
            }
            case "windowed": {
                IncrementalSummarizer summarizer = new IncrementalSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);

                WindowedOperator<FPGExplanation> windowedSummarizer = new WindowedOperator<>(summarizer);
                windowedSummarizer.setWindowLength(windowLength);
                windowedSummarizer.setSlideLength(slideLength);
                windowedSummarizer.setTimeColumn(timeColumn);
                windowedSummarizer.initialize();

                return windowedSummarizer;
            }
            default: {
                throw new MacroBaseException("Bad Summarizer Type");
            }
        }
    }
}
