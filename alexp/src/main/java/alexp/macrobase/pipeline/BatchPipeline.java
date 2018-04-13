package alexp.macrobase.pipeline;

import alexp.macrobase.ingest.SqlDataFrameReader;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.ingest.XlsxDataFrameReader;
import com.google.common.base.Stopwatch;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.GlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.RiskRatioMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BatchPipeline implements Pipeline {
    private final Uri inputURI;

    private String sqlQuery;

    private String classifierType;
    private String metric;
    private double cutoff;
    private String strCutoff;
    private boolean isStrPredicate;
    private boolean pctileHigh;
    private boolean pctileLow;
    private String predicateStr;
    private int numThreads;

    private String summarizerType;
    private List<String> attributes;
    private String ratioMetric;
    private double minSupport;
    private double minRiskRatio;

    public BatchPipeline(PipelineConfig conf) {
        inputURI = new Uri(conf.get("inputURI"));

        classifierType = conf.get("classifier", "percentile");
        metric = conf.get("metric");

        if (inputURI.getType() == Uri.Type.JDBC) {
            sqlQuery = conf.get("query");
        }

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
        ratioMetric = conf.get("ratioMetric", "globalRatio");
        minRiskRatio = conf.get("minRatioMetric", 3.0);
        minSupport = conf.get("minSupport", 0.01);

        numThreads = conf.get("numThreads", Runtime.getRuntime().availableProcessors());
    }

    @Override
    public Explanation results() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();

        DataFrame df = loadData();

        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);
        sw = Stopwatch.createStarted();

        Classifier classifier = getClassifier();
        classifier.process(df);
        df = classifier.getResults();

        final long classifierMs = sw.elapsed(TimeUnit.MILLISECONDS);
        sw = Stopwatch.createStarted();

        BatchSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());
        summarizer.process(df);
        Explanation explanation = summarizer.getResults();

        final long explanationMs = sw.elapsed(TimeUnit.MILLISECONDS);

        System.out.printf("Load time: %d ms\nClassification time: %d ms\nSummarization time: %d ms\n", loadMs, classifierMs, explanationMs);

        return explanation;
    }

    private DataFrame loadDataFrame(Uri inputURI, Map<String, Schema.ColType> colTypes, List<String> requiredColumns) throws Exception {
        switch (inputURI.getType()) {
            case XLSX:
                return new XlsxDataFrameReader(inputURI.getPath(), requiredColumns, 0).load();
            case JDBC:
                return new SqlDataFrameReader(inputURI.getPath(), requiredColumns, sqlQuery).load();
            default:
                return PipelineUtils.loadDataFrame(inputURI.getOriginalString(), colTypes, requiredColumns);
        }
    }

    private DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(metric);
        return loadDataFrame(inputURI, colTypes, requiredColumns);
    }

    private Classifier getClassifier() throws MacroBaseException {
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
                throw new MacroBaseException("Bad Classifier Type");
            }
        }
    }

    private ExplanationMetric getRatioMetric() throws MacroBaseException {
        switch (ratioMetric.toLowerCase()) {
            case "globalratio": {
                return new GlobalRatioMetric();
            }
            case "riskratio": {
                return new RiskRatioMetric();
            }
            default: {
                throw new MacroBaseException("Bad Ratio Metric");
            }
        }
    }

    private BatchSummarizer getSummarizer(String outlierColumnName) throws MacroBaseException {
        switch (summarizerType.toLowerCase()) {
            case "fpgrowth": {
                FPGrowthSummarizer summarizer = new FPGrowthSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRiskRatio(minRiskRatio);
                summarizer.setUseAttributeCombinations(true);
                summarizer.setNumThreads(numThreads);
                return summarizer;
            }
            case "apriori":
            case "aplinear": {
                APLOutlierSummarizer summarizer = new APLOutlierSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRatioMetric(minRiskRatio);
                summarizer.setNumThreads(numThreads);
                return summarizer;
            }
            default: {
                throw new MacroBaseException("Bad Summarizer Type");
            }
        }
    }
}
