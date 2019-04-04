package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class ExecutionResult {
    private final long trainingTime;
    private final long classificationTime;
    private final long maxMemory;
    private final BenchmarkConfig benchmarkConfig;

    public ExecutionResult(long trainingTime, long classificationTime, long maxMemory, BenchmarkConfig benchmarkConfig) {
        this.trainingTime = trainingTime;
        this.classificationTime = classificationTime;
        this.maxMemory = maxMemory;
        this.benchmarkConfig = benchmarkConfig;
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

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "config", benchmarkConfig.toMap().getValues(),
                "result", ImmutableMap.of(
                        "trainingTime", trainingTime,
                        "classificationTime", classificationTime,
                        "maxMemory", maxMemory
                )
        ));
    }
}
