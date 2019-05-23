package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
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
            "classifiers", Lists.newArrayList(
                    ImmutableMap.of(
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
                            )),
                    ImmutableMap.of("id", "fastmcd")
            )
    );

    @Test
    public void loadsFromMap() throws IOException {
        BenchmarkConfig config = BenchmarkConfig.load(new StringObjectMap(configValues));

        assertEquals("csv://shuttle.csv", config.getDatasetConfig().getUri().getOriginalString());
        assertEquals("shuttle dataset", config.getDatasetConfig().getDatasetId());
        assertEquals("is_anomaly", config.getDatasetConfig().getLabelColumn());
        assertArrayEquals(new String[]{"d1", "d2"}, config.getDatasetConfig().getMetricColumns());

        AlgorithmConfig alg1 = config.getClassifierConfig("iforest");
        assertEquals("iforest", alg1.getAlgorithmId());
        assertEquals(ImmutableMap.of(
                "treesCount", 100,
                "param2", 3.14
        ), alg1.getParameters().getValues());
        GridSearchConfig gsConfig = alg1.getGridSearchConfig();
        assertNotNull(gsConfig);
        assertEquals("pr", gsConfig.getMeasure());
        assertArrayEquals(Lists.newArrayList(50, 100, 150).toArray(), gsConfig.getParameters().get("treesCount"));
        assertArrayEquals(Lists.newArrayList(1.0, 2.0, 2.5, 3.0).toArray(), gsConfig.getParameters().get("param3"));
        assertArrayEquals(Lists.newArrayList("mode1", "mode2").toArray(), gsConfig.getParameters().get("param4"));

        AlgorithmConfig alg2 = config.getClassifierConfig("fastmcd");
        assertEquals("fastmcd", alg2.getAlgorithmId());
        assertEquals(StringObjectMap.empty().getValues(), alg2.getParameters().getValues());
        assertNull(alg2.getGridSearchConfig());

        try {
            config.getClassifierConfig("notexisting");
            fail("getClassifierConfig should throw");
        } catch (Exception ignored) {}
    }

    @Test
    public void retrievesExecutionConfig() throws IOException {
        BenchmarkConfig mainConfig = BenchmarkConfig.load(new StringObjectMap(configValues));

        ExecutionConfig config = mainConfig.getExecutionConfig("iforest");

        assertEquals("csv://shuttle.csv", config.getDatasetConfig().getUri().getOriginalString());
        assertEquals("shuttle dataset", config.getDatasetConfig().getDatasetId());
        assertEquals("is_anomaly", config.getDatasetConfig().getLabelColumn());
        assertArrayEquals(new String[]{"d1", "d2"}, config.getDatasetConfig().getMetricColumns());

        AlgorithmConfig alg = config.getClassifierConfig();
        assertEquals("iforest", alg.getAlgorithmId());
        assertEquals(ImmutableMap.of(
                "treesCount", 100,
                "param2", 3.14
        ), alg.getParameters().getValues());
        GridSearchConfig gsConfig = alg.getGridSearchConfig();
        assertNotNull(gsConfig);
        assertEquals("pr", gsConfig.getMeasure());
        assertArrayEquals(Lists.newArrayList(50, 100, 150).toArray(), gsConfig.getParameters().get("treesCount"));
        assertArrayEquals(Lists.newArrayList(1.0, 2.0, 2.5, 3.0).toArray(), gsConfig.getParameters().get("param3"));
        assertArrayEquals(Lists.newArrayList("mode1", "mode2").toArray(), gsConfig.getParameters().get("param4"));
    }

    @Test
    public void savesResultToFile() throws IOException {
        BenchmarkConfig mainConfig = BenchmarkConfig.load(new StringObjectMap(configValues));

        ExecutionConfig config = mainConfig.getExecutionConfig("iforest");

        Map<String, Object> expectedResultConfigValues = ImmutableMap.of(
                "dataset", ImmutableMap.of(
                        "uri", "csv://shuttle.csv",
                        "id", "shuttle dataset",
                        "metricColumns", Lists.newArrayList("d1", "d2"),
                        "labelColumn", "is_anomaly"
                ),
                "classifier", ImmutableMap.of(
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

        String resultFilePath = tmpFolder.getRoot().getAbsolutePath() + "/config.json";
        config.toMap().toJsonFile(resultFilePath);

        StringObjectMap loadedConfig = StringObjectMap.fromJsonFile(resultFilePath);
        assertEquals(expectedResultConfigValues, loadedConfig.getValues());
    }
}