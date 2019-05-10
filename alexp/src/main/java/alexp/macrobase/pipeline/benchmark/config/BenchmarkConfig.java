package alexp.macrobase.pipeline.benchmark.config;


import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.DatasetConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BenchmarkConfig {
    private final List<AlgorithmConfig> classifierConfigs;
    private final List<AlgorithmConfig> explanationConfigs;
    private final DatasetConfig datasetConfig;
    private final SettingsConfig settingsConfig;

    private static final String CLASSIFIERS_CONF_TAG    =   "classifiers";
    private static final String EXPLAINERS_CONF_TAG     =   "explainers";
    private static final String DATASET_CONF_TAG        =   "dataset";
    private static final String SETTINGS_CONF_TAG       =   "settingsConfigPath";

    public BenchmarkConfig(List<AlgorithmConfig> classifierConfigs, List<AlgorithmConfig> explanationConfigs,
                           DatasetConfig datasetConfig, SettingsConfig settingsConfig) {
        this.classifierConfigs = classifierConfigs;
        this.explanationConfigs = explanationConfigs;
        this.datasetConfig = datasetConfig;
        this.settingsConfig = settingsConfig;
    }

    public static BenchmarkConfig load(StringObjectMap conf) {
        return new BenchmarkConfig(getClassifierConfigList(conf),
                getExplanationConfigList(conf),
                DatasetConfig.load(conf.getMap(DATASET_CONF_TAG)),
                SettingsConfig.load(conf.get(SETTINGS_CONF_TAG)));
    }

    public StringObjectMap toMap() {
        if(explanationConfigs.isEmpty())
            return new StringObjectMap(ImmutableMap.of(
                    CLASSIFIERS_CONF_TAG, confToMap(CLASSIFIERS_CONF_TAG).getValues(),
                    DATASET_CONF_TAG, datasetConfig.toMap().getValues()
            ));
        else
            return new StringObjectMap(ImmutableMap.of(
                CLASSIFIERS_CONF_TAG, confToMap(CLASSIFIERS_CONF_TAG).getValues(),
                EXPLAINERS_CONF_TAG, confToMap(EXPLAINERS_CONF_TAG),
                DATASET_CONF_TAG, datasetConfig.toMap().getValues()
        ));
    }

    private static List<AlgorithmConfig> getClassifierConfigList(StringObjectMap conf) {
        List<AlgorithmConfig> classifierConfs = new ArrayList<>();
        List<StringObjectMap> objectMapList = conf.getMapList(CLASSIFIERS_CONF_TAG);
        for(StringObjectMap o : objectMapList){
            classifierConfs.add(AlgorithmConfig.load(o));
        }
        return classifierConfs;
    }

    private static List<AlgorithmConfig> getExplanationConfigList(StringObjectMap conf){
        List<AlgorithmConfig> explainerConfs = new ArrayList<>();
        List<StringObjectMap> objectMapList = conf.getMapList(EXPLAINERS_CONF_TAG);
        for(StringObjectMap o : objectMapList){
            explainerConfs.add(AlgorithmConfig.load(o));
        }
        return explainerConfs;
    }

    private StringObjectMap confToMap(String option) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        List<Object> values = new ArrayList<>();
        for(AlgorithmConfig cconf : option.equals(CLASSIFIERS_CONF_TAG) ? classifierConfigs : explanationConfigs){
            values.add(cconf.toMap());
        }
        mapBuilder.put(option,values);
        return new StringObjectMap(mapBuilder.build());
    }

    public List<AlgorithmConfig> getClassifierConfigs() {
        return classifierConfigs;
    }

    public List<AlgorithmConfig> getExplanationConfigs() {
        return explanationConfigs;
    }

    public AlgorithmConfig getClassifierConfig(String id) {
        List<AlgorithmConfig> configList = classifierConfigs.stream().filter(x -> x.getAlgorithmId().equals(id)).collect(Collectors.toList());
        if(configList.size() != 1)
            throw new RuntimeException("Error in getting classifier id " + id + " from classifiers list " + classifierConfigs);
        return configList.get(0);
    }

    public AlgorithmConfig getExplanationConfig(String id) {
        List<AlgorithmConfig> configList = explanationConfigs.stream().filter(x -> x.getAlgorithmId().equals(id)).collect(Collectors.toList());
        if(configList.size() != 1)
            throw new RuntimeException("Error in getting explainer id " + id + " from explainers list " + classifierConfigs);
        return configList.get(0);
    }

    public BenchmarkConfig getBenchConfForClassifier(String id) {
        List<AlgorithmConfig> classifiersConfList = classifierConfigs.stream().filter(x -> x.getAlgorithmId().equals(id)).collect(Collectors.toList());
        if(classifiersConfList.size() != 1)
            throw new RuntimeException("Error in getting classifier id " + id + " from classifiers list " + classifierConfigs);
        List<AlgorithmConfig> cconf = Arrays.asList(classifiersConfList.get(0));
        return new BenchmarkConfig(cconf, explanationConfigs, datasetConfig, settingsConfig);
    }

    public BenchmarkConfig getBenchConfForExplainer(String id) {
        List<AlgorithmConfig> exlpainersConfList = explanationConfigs.stream().filter(x -> x.getAlgorithmId().equals(id)).collect(Collectors.toList());
        if(exlpainersConfList.size() != 1)
            throw new RuntimeException("Error in getting explainer id " + id + " from explainers list " + classifierConfigs);
        List<AlgorithmConfig> econf = Arrays.asList(exlpainersConfList.get(0));
        return new BenchmarkConfig(classifierConfigs, econf, datasetConfig, settingsConfig);
    }

    public SettingsConfig getSettingsConfig() {
        return settingsConfig;
    }

    public DatasetConfig getDatasetConfig() {
        return datasetConfig;
    }
}
