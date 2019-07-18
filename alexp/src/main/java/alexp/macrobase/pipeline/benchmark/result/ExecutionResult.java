package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import alexp.macrobase.pipeline.benchmark.config.ExecutionConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class ExecutionResult {
    private final long trainingTime;
    private final long classificationTime;
    private final long updateTime;
    private final long maxMemory;
    private final ExecutionConfig executionConfig;
    private final StringObjectMap finalAlgorithmConfig;

    // maybe add to the output?
    private String classifierId;
    private String explainerId;

    public ExecutionResult(long trainingTime, long classificationTime, long updateTime, long maxMemory, ExecutionConfig executionConfig,
                           StringObjectMap finalAlgorithmConfig) {
        this.trainingTime = trainingTime;
        this.classificationTime = classificationTime;
        this.updateTime = updateTime;
        this.maxMemory = maxMemory;
        this.executionConfig = executionConfig;
        this.finalAlgorithmConfig = finalAlgorithmConfig;
    }

    public long getTrainingTime() {
        return trainingTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public long getClassificationTime() {
        return classificationTime;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    public StringObjectMap getFinalAlgorithmConfig() {
        return finalAlgorithmConfig;
    }

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "config", executionConfig.toMap().getValues(),
                "result", ImmutableMap.of(
                        "trainingTime", trainingTime,
                        "classificationTime", classificationTime,
                        "updateTime", updateTime,
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
