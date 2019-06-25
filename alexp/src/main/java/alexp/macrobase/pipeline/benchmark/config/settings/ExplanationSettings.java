package alexp.macrobase.pipeline.benchmark.config.settings;

import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import spark.utils.Assert;

import java.util.List;

public class ExplanationSettings {

    private final String                method;
    private final boolean               invokePythonClassifier;
    private final List<Integer>         dictatedOutliers;
    private final AlgorithmConfig       classifierConf;
    private final double                threshold;

    private final static String         METHOD_TAG                      =   "method";
    private final static String         INVOKE_PYTHON_CLASSIFIER_TAG    =   "invokePythonClassifier";
    private final static String         DICTATED_OUTLIERS_TAG           =   "dictatedOutliers";
    private final static String         DETECTED_OUTLIERS_TAG           =   "detectedOutliers";
    private final static String         CLASSIFIER_TAG                  =   "classifier";
    private final static String         THRESHOLD_TAG                   =   "threshold";


    private ExplanationSettings(String method,
                               boolean invokePythonClassifier,
                               List<Integer> dictatedOutliers,
                               AlgorithmConfig classifierConf,
                               double threshold) {
        this.method                 =   method;
        this.invokePythonClassifier =   invokePythonClassifier;
        this.dictatedOutliers       =   dictatedOutliers;
        this.classifierConf         =   classifierConf;
        this.threshold              =   threshold;
        System.out.println("Invoke classifier from python: " + invokePythonClassifier);
        validateConfig();
    }

    public static ExplanationSettings load(StringObjectMap explSettingsConf) {
        Assert.notNull(explSettingsConf);
        return new ExplanationSettings(
                explSettingsConf.get(METHOD_TAG),
                explSettingsConf.get(INVOKE_PYTHON_CLASSIFIER_TAG),
                explSettingsConf.get(DICTATED_OUTLIERS_TAG),
                AlgorithmConfig.load(explSettingsConf.getMap(DETECTED_OUTLIERS_TAG).getMap(CLASSIFIER_TAG)),
                explSettingsConf.getMap(DETECTED_OUTLIERS_TAG).get(THRESHOLD_TAG)
        );
    }

    public String getMethod() {
        return method;
    }

    public boolean invokePythonClassifier() { return invokePythonClassifier; }

    public List<Integer> getDictatedOutliers() {
        return dictatedOutliers;
    }

    public AlgorithmConfig getClassifierConf() {
        return classifierConf;
    }

    public boolean dictatedOutlierMethod() {
        return method.equals(DICTATED_OUTLIERS_TAG);
    }

    public boolean detectedOutlierMethod() {
        return method.equals(DETECTED_OUTLIERS_TAG);
    }

    public double getThreshold() {
        return threshold;
    }

    private void validateConfig() {
        if(method == null || !(dictatedOutlierMethod() || detectedOutlierMethod()))
            throw new RuntimeException("Method in settings configuration" +
                    " should be either \"" + DICTATED_OUTLIERS_TAG + "\" or \"" + DETECTED_OUTLIERS_TAG + "\"");
        if(dictatedOutlierMethod() && (dictatedOutliers == null || dictatedOutliers.isEmpty()))
            throw new RuntimeException("Method " + DICTATED_OUTLIERS_TAG + " needs the IDs of points to be explained by the algorithm");
        if(detectedOutlierMethod()) {
            if(classifierConf == null)
                throw new RuntimeException("Method " + DETECTED_OUTLIERS_TAG + " needs a classifier to detect the outliers");
            else if(threshold < 0)
                throw new RuntimeException("Threshold " + threshold + " is not valid (<0)");
        }
    }

}