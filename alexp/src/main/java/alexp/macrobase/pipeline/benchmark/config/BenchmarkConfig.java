package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class BenchmarkConfig {
    private final AlgorithmConfig algorithmConfig;
    private final DatasetConfig datasetConfig;

    public BenchmarkConfig(AlgorithmConfig algorithmConfig, DatasetConfig datasetConfig) {
        this.algorithmConfig = algorithmConfig;
        this.datasetConfig = datasetConfig;
    }

    public static BenchmarkConfig load(StringObjectMap conf) {
        return new BenchmarkConfig(AlgorithmConfig.load(conf.getMap("algorithm")), DatasetConfig.load(conf.getMap("dataset")));
    }

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "algorithm", algorithmConfig.toMap().getValues(),
                "dataset", datasetConfig.toMap().getValues()
        ));
    }

    public AlgorithmConfig getAlgorithmConfig() {
        return algorithmConfig;
    }

    public DatasetConfig getDatasetConfig() {
        return datasetConfig;
    }
}
