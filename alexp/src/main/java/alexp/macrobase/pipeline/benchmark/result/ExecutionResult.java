package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class ExecutionResult {
    private final long timeElapsed;
    private final BenchmarkConfig benchmarkConfig;

    public ExecutionResult(long timeElapsed, BenchmarkConfig benchmarkConfig) {
        this.timeElapsed = timeElapsed;
        this.benchmarkConfig = benchmarkConfig;
    }

    public long getTimeElapsed() {
        return timeElapsed;
    }

    public BenchmarkConfig getBenchmarkConfig() {
        return benchmarkConfig;
    }

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "config", benchmarkConfig.toMap().getValues(),
                "result", ImmutableMap.of(
                        "timeElapsed", timeElapsed
                )
        ));
    }
}
