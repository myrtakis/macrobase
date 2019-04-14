package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.*;

public class BenchmarkConfigTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

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

    private final Map<String, Object> config2Values = ImmutableMap.of(
            "dataset", ImmutableMap.of(
                    "uri", "csv://mulcross.csv",
                    "id", "mulcross dataset",
                    "metricColumns", Lists.newArrayList("d1", "d2")
                    // no label
            ),
            "algorithm", ImmutableMap.of(
                    "id", "iforest",
                    "parameters", ImmutableMap.of(
                            "treesCount", 100,
                            "param2", 3.14
                    ))
    );

    private final Map<String, Object> configGsValues = ImmutableMap.of(
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
                    ),
                    "gridsearch", ImmutableMap.of(
                            "measure", "pr",
                            "parameters", ImmutableMap.of(
                                    "treesCount", Lists.newArrayList(50, 100, 150),
                                    "param3", Lists.newArrayList(1.0, 2.0, 2.5, 3.0),
                                    "param4", Lists.newArrayList("mode1", "mode2")
                            )
                    ))
    );

    @Test
    public void loadsFromMap() {
        BenchmarkConfig config = BenchmarkConfig.load(new StringObjectMap(configValues));

        assertEquals("csv://shuttle.csv", config.getDatasetConfig().getUri().getOriginalString());
        assertEquals("shuttle dataset", config.getDatasetConfig().getDatasetId());
        assertEquals("is_anomaly", config.getDatasetConfig().getLabelColumn());
        assertArrayEquals(new String[]{"d1", "d2"}, config.getDatasetConfig().getMetricColumns());
        assertEquals("iforest", config.getAlgorithmConfig().getAlgorithmId());
        assertEquals(ImmutableMap.of(
                "treesCount", 100,
                "param2", 3.14
        ), config.getAlgorithmConfig().getParameters().getValues());
        assertNull(config.getAlgorithmConfig().getGridSearchConfig());
    }

    @Test
    public void savesToMap() {
        assertEquals(configValues, BenchmarkConfig.load(new StringObjectMap(configValues)).toMap().getValues());
    }

    @Test
    public void worksWithoutLabel() {
        BenchmarkConfig config = BenchmarkConfig.load(new StringObjectMap(config2Values));

        assertEquals("csv://mulcross.csv", config.getDatasetConfig().getUri().getOriginalString());
        assertNull(config.getDatasetConfig().getLabelColumn());

        assertEquals(config2Values, BenchmarkConfig.load(new StringObjectMap(config2Values)).toMap().getValues());
    }

    @Test
    public void loadsSavesGs() {
        BenchmarkConfig config = BenchmarkConfig.load(new StringObjectMap(configGsValues));

        GridSearchConfig gsConfig = config.getAlgorithmConfig().getGridSearchConfig();
        assertNotNull(gsConfig);
        assertEquals("pr", gsConfig.getMeasure());
        assertArrayEquals(Lists.newArrayList(50, 100, 150).toArray(), gsConfig.getParameters().get("treesCount"));
        assertArrayEquals(Lists.newArrayList(1.0, 2.0, 2.5, 3.0).toArray(), gsConfig.getParameters().get("param3"));
        assertArrayEquals(Lists.newArrayList("mode1", "mode2").toArray(), gsConfig.getParameters().get("param4"));

        assertEquals(configGsValues, BenchmarkConfig.load(new StringObjectMap(configGsValues)).toMap().getValues());
    }

    @Test
    public void savesToFile() throws IOException {
        String inputFilePath = tmpFolder.getRoot().getAbsolutePath() + "/config.yaml";
        new StringObjectMap(configGsValues).toYamlFile(inputFilePath);

        BenchmarkConfig loadedConfig = BenchmarkConfig.load(StringObjectMap.fromYamlFile(inputFilePath));
        assertEquals(configGsValues, loadedConfig.toMap().getValues());

        String resultFilePath = tmpFolder.getRoot().getAbsolutePath() + "/config.json";
        loadedConfig.toMap().toJsonFile(resultFilePath);

        BenchmarkConfig loadedConfig2 = BenchmarkConfig.load(StringObjectMap.fromJsonFile(resultFilePath));
        assertEquals(configGsValues, loadedConfig2.toMap().getValues());
    }
}