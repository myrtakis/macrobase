package alexp.macrobase.pipeline.benchmark.config;


import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.DatasetConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BenchmarkConfig {
    private final List<AlgorithmConfig> classifierConfigs;
    private final List<AlgorithmConfig> explanationConfigs;
    private final DatasetConfig datasetConfig;
    private final SettingsConfig settingsConfig;

    private static final String CLASSIFIERS_CONF_TAG    =   "classifiers";
    private static final String EXPLAINERS_CONF_TAG     =   "explainers";
    static final String DATASET_CONF_TAG        =   "dataset";
    static final String SETTINGS_CONF_TAG       =   "settingsConfigPath";

    public BenchmarkConfig(List<AlgorithmConfig> classifierConfigs, List<AlgorithmConfig> explanationConfigs,
                           DatasetConfig datasetConfig, SettingsConfig settingsConfig) {
        this.classifierConfigs = classifierConfigs;
        this.explanationConfigs = explanationConfigs;
        this.datasetConfig = datasetConfig;
        this.settingsConfig = settingsConfig;
    }

    public static BenchmarkConfig load(StringObjectMap conf) throws IOException {
        return new BenchmarkConfig(getClassifierConfigList(conf),
                getExplanationConfigList(conf),
                DatasetConfig.load(conf.getMap(DATASET_CONF_TAG)),
                SettingsConfig.load(conf.get(SETTINGS_CONF_TAG)));
    }

    public StringObjectMap toMap() {
        if(explanationConfigs.isEmpty())
            return new StringObjectMap(ImmutableMap.of(
                    CLASSIFIERS_CONF_TAG, classifierConfigs.stream().map(c -> c.toMap().getValues()).collect(Collectors.toList()),
                    DATASET_CONF_TAG, datasetConfig.toMap().getValues()
            ));
        else
            return new StringObjectMap(ImmutableMap.of(
                CLASSIFIERS_CONF_TAG, classifierConfigs.stream().map(c -> c.toMap().getValues()).collect(Collectors.toList()),
                EXPLAINERS_CONF_TAG, explanationConfigs.stream().map(c -> c.toMap().getValues()).collect(Collectors.toList()),
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

    public ExecutionConfig getExecutionConfig(String classifierId, String explainerId) {
        AlgorithmConfig classifierConf = classifierConfigs.stream().filter(it -> it.getAlgorithmId().equals(classifierId)).findFirst().orElse(null);
        AlgorithmConfig explainerConf = explanationConfigs.stream().filter(it -> it.getAlgorithmId().equals(explainerId)).findFirst().orElse(null);
        return getExecutionConfig(classifierConf, explainerConf);
    }

    public ExecutionConfig getExecutionConfig(String classifierId) {
        return getExecutionConfig(classifierId);
    }

    public ExecutionConfig getExecutionConfig(AlgorithmConfig classifierConf, AlgorithmConfig explainerConf) {
        return new ExecutionConfig(classifierConf, explainerConf, datasetConfig, settingsConfig);
    }

    public ExecutionConfig getExecutionConfig(AlgorithmConfig classifierConf) {
        return getExecutionConfig(classifierConf, null);
    }

    public SettingsConfig getSettingsConfig() {
        return settingsConfig;
    }

    public DatasetConfig getDatasetConfig() {
        return datasetConfig;
    }
}
