package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class BenchmarkConfigTest {

    private final Map<String, Object> configValues = ImmutableMap.of(
            "dataset", ImmutableMap.of(
                    "uri", "csv://shuttle.csv",
                    "id", "shuttle dataset",
                    "metricColumns", Lists.newArrayList("d1", "d2"),
                    "labelColumn", "is_anomaly"
            ),
            "algorithm", ImmutableMap.of(
                    "id", "iforest",
                    "parameters", ImmutableMap.of(
                            "treesCount", 100,
                            "param2", 3.14
                    ))
    );

    @Test
    public void loadsFromMap() {
        BenchmarkConfig config = BenchmarkConfig.load(new StringObjectMap(configValues));

        assertEquals("csv://shuttle.csv", config.getDatasetConfig().getUri().getOriginalString());
        assertEquals("shuttle dataset", config.getDatasetConfig().getDatasetId());
        assertArrayEquals(new String[]{"d1", "d2"}, config.getDatasetConfig().getMetricColumns());
        assertEquals("iforest", config.getAlgorithmConfig().getAlgorithmId());
        assertEquals(ImmutableMap.of(
                "treesCount", 100,
                "param2", 3.14
        ), config.getAlgorithmConfig().getParameters().getValues());
    }

    @Test
    public void savesToMap() {
        assertEquals(configValues, BenchmarkConfig.load(new StringObjectMap(configValues)).toMap().getValues());
    }
}