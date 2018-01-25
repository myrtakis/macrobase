package alexp.macrobase.pipeline;

import alexp.macrobase.ingest.HttpCsvStreamReader;
import alexp.macrobase.ingest.StreamingDataFrameLoader;
import alexp.macrobase.ingest.Uri;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class StreamingPipeline {
    private final Uri inputURI;

    private String classifierType;
    private String metric;
    private double cutoff;
    private String strCutoff;
    private boolean isStrPredicate;
    private boolean pctileHigh;
    private boolean pctileLow;
    private String predicateStr;

    private String summarizerType;
    private List<String> attributes;
    private double minSupport;

    private Integer numPanes;

    public StreamingPipeline(PipelineConfig conf) {
        inputURI = new Uri(conf.get("inputURI"));

        classifierType = conf.get("classifier", "percentile");
        metric = conf.get("metric");

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

        summarizerType = conf.get("summarizer", "apriori");
        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 0.01);

        numPanes = conf.get("numPanes", 3);
    }

    public void run(Consumer<Explanation> resultCallback) throws Exception {
        StreamingDataFrameLoader dataLoader = getDataLoader();

        Classifier classifier = getClassifier();
        IncrementalSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());

        dataLoader.load(dataFrame -> {
            classifier.process(dataFrame);

            summarizer.process(classifier.getResults());

            resultCallback.accept(summarizer.getResults());
        });
    }

    private StreamingDataFrameLoader getDataLoader() throws Exception {
        if (inputURI.getType() != Uri.Type.HTTP) {
            throw new Exception("Unsupported input protocol");
        }

        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(metric);

        return new HttpCsvStreamReader(inputURI.getPath(), requiredColumns)
                .setColumnTypes(colTypes);
    }

    private Classifier getClassifier() throws MacrobaseException {
        switch (classifierType.toLowerCase()) {
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
                throw new MacrobaseException("Bad Classifier Type");
            }
        }
    }

    private IncrementalSummarizer getSummarizer(String outlierColumnName) throws MacrobaseException {
        switch (summarizerType.toLowerCase()) {
            case "incremental": {
                IncrementalSummarizer summarizer = new IncrementalSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setWindowSize(numPanes);
                return summarizer;
            }
            default: {
                throw new MacrobaseException("Bad Summarizer Type");
            }
        }
    }
}
