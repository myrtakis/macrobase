package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class ExecutionConfig {
    private final AlgorithmConfig classifierConfig;
    private final AlgorithmConfig explainerConfig;
    private final DatasetConfig datasetConfig;
    private final SettingsConfig settingsConfig;

    public ExecutionConfig(AlgorithmConfig classifierConfig, AlgorithmConfig explainerConfig, DatasetConfig datasetConfig, SettingsConfig settingsConfig) {
        this.classifierConfig = classifierConfig;
        this.explainerConfig = explainerConfig;
        this.datasetConfig = datasetConfig;
        this.settingsConfig = settingsConfig;
    }

    public ExecutionConfig(AlgorithmConfig classifierConfig, DatasetConfig datasetConfig, SettingsConfig settingsConfig) {
        this(classifierConfig, null, datasetConfig, settingsConfig);
    }

    public ExecutionConfig(AlgorithmConfig classifierConfig, DatasetConfig datasetConfig) {
        this(classifierConfig, null, datasetConfig, null);
    }

    public StringObjectMap toMap() {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        mapBuilder.putAll(ImmutableMap.of(
                "classifier", classifierConfig.toMap().getValues(),
                BenchmarkConfig.DATASET_CONF_TAG, datasetConfig.toMap().getValues()
        ));
        if (explainerConfig != null) {
            mapBuilder.put("explainer", explainerConfig.toMap().getValues());
        }
        if (settingsConfig != null) {
            // TODO: implement settingsConfig.toMap() if it's needed
            //mapBuilder.put(BenchmarkConfig.SETTINGS_CONF_TAG, settingsConfig.toMap().getValues());
        }
        return new StringObjectMap(mapBuilder.build());
    }

    public AlgorithmConfig getClassifierConfig() {
        return classifierConfig;
    }

    public AlgorithmConfig getExplainerConfig() {
        return explainerConfig;
    }

    public DatasetConfig getDatasetConfig() {
        return datasetConfig;
    }

    public SettingsConfig getSettingsConfig() {
        return settingsConfig;
    }
}
