package alexp.macrobase.pipeline;

import alexp.macrobase.explanation.Itemset;
import alexp.macrobase.ingest.*;
import alexp.macrobase.normalization.MinMaxNormalizer;
import alexp.macrobase.normalization.Normalizer;
import alexp.macrobase.outlier.MAD;
import alexp.macrobase.outlier.MinCovDet;
import alexp.macrobase.outlier.RandomClassifier;
import alexp.macrobase.outlier.iforest.IsolationForest;
import alexp.macrobase.outlier.lof.bkaluza.LOF;
import alexp.macrobase.outlier.lof.chen.LOCI;
import alexp.macrobase.outlier.mcod.McodClassifier;
import alexp.macrobase.utils.ConfigUtils;
import alexp.macrobase.utils.TimeUtils;
import com.google.common.collect.Iterables;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.operator.Operator;
import edu.stanford.futuredata.macrobase.operator.Transformer;
import edu.stanford.futuredata.macrobase.operator.WindowedOperator;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class Pipelines {
    public static DataFrame loadDataFrame(
            Uri inputURI,
            Map<String, Schema.ColType> colTypes,
            List<String> requiredColumns,
            PipelineConfig conf) throws Exception {
        requiredColumns = requiredColumns.stream().filter(c -> c != null && !isAutoGeneratedColumn(c)).collect(Collectors.toList());

        switch (inputURI.getType()) {
            case XLSX:
                return new XlsxDataFrameReader(inputURI.getPath(), requiredColumns, 0).load();
            case JDBC:
                return new SqlDataFrameReader(inputURI.getPath(), requiredColumns, conf.get("query")).load();
            default:
                return PipelineUtils.loadDataFrame(inputURI.getOriginalString(), colTypes, requiredColumns);
        }
    }

    public static StreamingDataFrameLoader getStreamingDataLoader(
            Uri inputURI,
            Map<String, Schema.ColType> colTypes,
            List<String> requiredColumns,
            PipelineConfig conf) throws Exception {
        requiredColumns = requiredColumns.stream().filter(c -> c != null && !isAutoGeneratedColumn(c)).collect(Collectors.toList());

        switch (inputURI.getType()) {
            case HTTP:
                return new HttpCsvStreamReader(inputURI.getPath(), requiredColumns)
                        .setColumnTypes(colTypes);
            case CSV:
                return new CsvStreamReader(inputURI.getPath(), requiredColumns)
                        .setColumnTypes(colTypes)
                        .setMaxBatchSize(conf.get("maxReadBatchSize", 5000));
            case JDBC:
                return new SqlStreamReader(inputURI.getPath(), requiredColumns, conf.get("query"), conf.get("idColumn", "id"))
                        .setColumnTypes(colTypes)
                        .setMaxBatchSize(conf.get("maxReadBatchSize", 5000));
            default:
                throw new Exception("Unsupported input protocol " + inputURI.getType());
        }
    }

    public static boolean canLoadColumns(Uri inputURI, PipelineConfig conf) throws Exception {
        if (inputURI.isDir()) {
            return false;
        }
        switch (inputURI.getType()) {
            case CSV:
                return true;
            default:
                return false;
        }
    }

    public static String[] loadColumns(Uri inputURI, PipelineConfig conf) throws Exception {
        switch (inputURI.getType()) {
            case CSV:
                return CsvStreamReader.loadColumnNames(inputURI.getPath());
            default:
                throw new Exception("Column loading is not supported for input protocol " + inputURI.getType());
        }
    }

    public static Classifier getClassifier(PipelineConfig conf, String[] metricColumns) throws MacroBaseException {
        String classifierType = conf.get("classifier");

        switch (classifierType.toLowerCase()) {
            case "mcod": {
                McodClassifier classifier = new McodClassifier(metricColumns);
                classifier.setMaxDistance(conf.get("maxDistance", 1.0));
                classifier.setMinNeighborCount(conf.get("minNeighborCount", 30));
                classifier.setWindowSize(conf.get("classifierWindowSize", 9999));
                classifier.setSlide(conf.get("classifierSlide", 9999));
                classifier.setAllowDuplicates(conf.get("allowDuplicates", false));
                classifier.setTimeColumnName(conf.get("timeColumn"));
                return classifier;
            }
            case "percentile": {
                PercentileClassifier classifier = new PercentileClassifier(metricColumns[0]);
                classifier.setPercentile(conf.get("cutoff", 1.0));
                classifier.setIncludeHigh(conf.get("includeHi",true));
                classifier.setIncludeLow(conf.get("includeLo",true));
                return classifier;
            }
            case "predicate": {
                String predicateStr = conf.get("predicate", "==").trim();
                Object rawCutoff = conf.get("cutoff");
                boolean isStrPredicate = rawCutoff instanceof String;
                if (isStrPredicate){
                    return new PredicateClassifier(metricColumns[0], predicateStr, (String) rawCutoff);
                }
                return new PredicateClassifier(metricColumns[0], predicateStr, conf.get("cutoff", 1.0));
            }
            case "mad": {
                MAD classifier = new MAD(metricColumns[0]);
                classifier.setTrainSize(conf.get("trainSize", 10000));
                Object threshold = conf.get("threshold");
                if (threshold != null) {
                    classifier.setThreshold((Double) threshold);
                }
                PipelineConfig normalizerConf = ConfigUtils.getObj(conf, "normalizer");
                if (normalizerConf != null) {
                    classifier.setNormalizer(getNormalizer(normalizerConf));
                }
                return classifier;
            }
            case "mcd": {
                MinCovDet classifier = new MinCovDet(metricColumns);
                classifier.setTrainSize(conf.get("trainSize", 10000));
                classifier.setAlpha(conf.get("alpha", 0.5));
                classifier.setStoppingDelta(conf.get("stoppingDelta", 0.001));
                return classifier;
            }
            case "loci": {
                LOCI classifier = new LOCI(metricColumns);
                classifier.setAlpha(conf.get("alpha", 0.5));
                classifier.setkSigma(conf.get("kSigma", 3));
                return classifier;
            }
            case "lof-chen": {
                alexp.macrobase.outlier.lof.chen.LOF classifier = new alexp.macrobase.outlier.lof.chen.LOF(metricColumns);
                classifier.setTrainSize(conf.get("trainSize", 10000));
                classifier.setParallel(conf.get("parallel", true));
                classifier.setSearchRange(conf.get("minPtsLB"), conf.get("minPtsUB"));
                return classifier;
            }
            case "lof-bkaluza": {
                LOF classifier = new LOF(metricColumns, LOF.Distance.EUCLIDIAN);
                classifier.setTrainSize(conf.get("trainSize", 100));
                classifier.setRetrainOnEachInput(conf.get("retrainOnEachInput", true));
                classifier.setkNN(conf.get("knn", 5));
                return classifier;
            }
            case "iforest": {
                IsolationForest classifier = new IsolationForest(metricColumns);
                classifier.setTrainSize(conf.get("trainSize", 100));
                classifier.setRetrainOnEachInput(conf.get("retrainOnEachInput", true));
                classifier.setTreesCount(conf.get("treesCount", 100));
                classifier.setSubsampleSize(conf.get("subsampleSize", 256));
                return classifier;
            }
            case "random": {
                RandomClassifier classifier = new RandomClassifier(metricColumns);
                classifier.setBinary(conf.get("binary", true));
                return classifier;
            }
            default: {
                throw new RuntimeException("Bad Classifier Type " + classifierType);
            }
        }
    }

    public static Operator<DataFrame, ? extends Explanation> getSummarizer(PipelineConfig conf, String outlierColumn, List<String> attributes) throws MacroBaseException {
        String summarizerType = conf.get("summarizer", "apriori");
        switch (summarizerType.toLowerCase()) {
            case "fpgrowth": {
                FPGrowthSummarizer summarizer = new FPGrowthSummarizer();
                summarizer.setOutlierColumn(outlierColumn);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(conf.get("minSupport", 0.01));
                summarizer.setMinRiskRatio(conf.get("minRatioMetric", 3.0));
                summarizer.setUseAttributeCombinations(true);
                summarizer.setNumThreads(conf.get("numThreads", Runtime.getRuntime().availableProcessors()));
                return summarizer;
            }
            case "apriori":
            case "aplinear": {
                APLOutlierSummarizer summarizer = new APLOutlierSummarizer();
                summarizer.setOutlierColumn(outlierColumn);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(conf.get("minSupport", 0.01));
                summarizer.setMinRatioMetric(conf.get("minRatioMetric", 3.0));
                summarizer.setNumThreads(conf.get("numThreads", Runtime.getRuntime().availableProcessors()));
                return summarizer;
            }
            case "incremental": {
                IncrementalSummarizer summarizer = new IncrementalSummarizer();
                summarizer.setOutlierColumn(outlierColumn);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(conf.get("minSupport", 0.01));
                summarizer.setWindowSize(conf.get("numPanes", 3));
                return summarizer;
            }
            case "windowed": {
                IncrementalSummarizer summarizer = new IncrementalSummarizer();
                summarizer.setOutlierColumn(outlierColumn);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(conf.get("minSupport", 0.01));

                WindowedOperator<FPGExplanation> windowedSummarizer = new WindowedOperator<>(summarizer);
                windowedSummarizer.setWindowLength(conf.get("windowLength", 6000));
                windowedSummarizer.setSlideLength(conf.get("slideLength", 1000));
                windowedSummarizer.setTimeColumn(conf.get("timeColumn"));
                windowedSummarizer.initialize();

                return windowedSummarizer;
            }
            default: {
                throw new MacroBaseException("Bad Summarizer Type " + summarizerType);
            }
        }
    }

    public static Normalizer getNormalizer(PipelineConfig conf) throws MacroBaseException {
        String type = conf.get("normalizer");

        switch (type.toLowerCase()) {
            case "minmax": {
                return new MinMaxNormalizer();
            }
            default: {
                throw new RuntimeException("Bad Normalizer Type " + type);
            }
        }
    }

    public static boolean isAutoGeneratedColumn(String column) {
        return column != null && column.startsWith("_");
    }

    public static void generateTimeColumn(DataFrame dataFrame, String column, long start) {
        double[] time = LongStream.rangeClosed(start, dataFrame.getNumRows() + start - 1).mapToDouble(n -> (double) n).toArray();
        dataFrame.addColumn(column, time);
    }

    public static String handleVariableColumnName(String[] columns, String column) {
        if (!isAutoGeneratedColumn(column)) {
            return column;
        }

        if (column.startsWith("_regex:")) {
            String regexStr = column.substring(column.indexOf(":") + 1);
            Pattern pattern = Pattern.compile(regexStr);

            return Arrays.stream(columns)
                    .filter(col -> pattern.matcher(col).matches())
                    .findFirst().get();
        }

        return column;
    }

    public static void parseTimeColumn(DataFrame dataFrame, String sourceColumn, String destColumn, String format) {
        String[] strValues = dataFrame.getStringColumnByName(sourceColumn);
        double[] time = Arrays.stream(strValues).mapToDouble(s -> TimeUtils.dateTimeToUnixTimestamp(s, format)).toArray();
        dataFrame.addColumn(destColumn, time);
    }

    public static List<Classifier> getClassifiersChain(List<PipelineConfig> classifierConfigs) throws MacroBaseException {
        ArrayList<Classifier> classifiers = new ArrayList<>();

        for (int i = 0; i < classifierConfigs.size(); i++) {
            PipelineConfig classifierConf = classifierConfigs.get(i);

            List<String> metricColumns = classifierConf.get("metricColumns", new ArrayList<String>());
            if (metricColumns.isEmpty()) {
                if (classifiers.isEmpty()) {
                    throw new MacroBaseException("Metric column(s) not specified");
                }
                metricColumns.add(Iterables.getLast(classifiers).getOutputColumnName());
            }

            Classifier classifier = getClassifier(classifierConf, metricColumns.toArray(new String[0]));
            if (i > 0) {
                classifier.setOutputColumnName("_OUTLIER" + i);
            }

            classifiers.add(classifier);
        }

        return classifiers;
    }

    public static <T extends Transformer> T processChained(DataFrame dataFrame, List<T> transformers) throws Exception {
        for (Transformer classifier : transformers) {
            classifier.process(dataFrame);
            dataFrame = classifier.getResults();
        }

        return Iterables.getLast(transformers);
    }

    public static List<Itemset> getItemsets(Explanation explanation) throws MacroBaseException {
        if (explanation instanceof APLExplanation) {
            return ((APLExplanation) explanation).results().stream()
                    .map(it -> new Itemset(it.get("matcher")))
                    .collect(Collectors.toList());
        } else if (explanation instanceof FPGExplanation) {
            return ((FPGExplanation) explanation).getItemsets().stream()
                    .map(it -> new Itemset(it.getItems()))
                    .collect(Collectors.toList());
        }
        throw new MacroBaseException("Unknown explanation type " + explanation.getClass().getName());
    }

    public static String getClassifierName(PipelineConfig conf) {
        return conf.get("classifier");
    }

    public static String classifierConfToString(PipelineConfig conf) {
        return conf.getValues().entrySet().stream().
                filter(it -> !it.getKey().equals("classifier") && !it.getKey().endsWith("Column"))
                .collect(Collectors.toSet())
                .toString();
    }
}
