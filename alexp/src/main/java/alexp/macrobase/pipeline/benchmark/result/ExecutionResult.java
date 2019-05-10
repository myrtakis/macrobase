package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class ExecutionResult {
    private final long trainingTime;
    private final long classificationTime;
    private final long maxMemory;
    private final BenchmarkConfig benchmarkConfig;
    private final StringObjectMap finalAlgorithmConfig;

    // maybe add to the output?
    private String classifierId;
    private String explainerId;

    public ExecutionResult(long trainingTime, long classificationTime, long maxMemory, BenchmarkConfig benchmarkConfig,
                           StringObjectMap finalAlgorithmConfig) {
        this.trainingTime = trainingTime;
        this.classificationTime = classificationTime;
        this.maxMemory = maxMemory;
        this.benchmarkConfig = benchmarkConfig;
        this.finalAlgorithmConfig = finalAlgorithmConfig;
    }

    public long getTrainingTime() {
        return trainingTime;
    }

    public long getClassificationTime() {
        return classificationTime;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public BenchmarkConfig getBenchmarkConfig() {
        return benchmarkConfig;
    }

    public StringObjectMap getFinalAlgorithmConfig() {
        return finalAlgorithmConfig;
    }

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "config", benchmarkConfig.toMap().getValues(),
                "result", ImmutableMap.of(
                        "trainingTime", trainingTime,
                        "classificationTime", classificationTime,
                        "maxMemory", maxMemory,
                        "finalAlgorithmConfig", ImmutableMap.of(
                                "parameters", finalAlgorithmConfig.getValues()
                        )
                )
        ));
    }

    public String getClassifierId() {
        return classifierId;
    }

    public ExecutionResult setClassifierId(String classifierId) {
        this.classifierId = classifierId;
        return this;
    }

    public String getExplainerId() {
        return explainerId;
    }

    public ExecutionResult setExplainerId(String explainerId) {
        this.explainerId = explainerId;
        return this;
    }
}
