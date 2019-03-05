package alexp.macrobase.pipeline.config;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class StringObjectMapTest {

    private final Map<String, Object> configValues = ImmutableMap.of(
            "inputURI", "csv://shuttle.csv",
            "metricColumns", Lists.newArrayList("d1", "d2"),
            "labelColumn", "is_anomaly",
            "algorithm", ImmutableMap.of(
                    "name", "iforest",
                    "parameters", ImmutableMap.of(
                            "treesCount", 100,
                            "param2", 3.14
                    ))
    );

    @Test
    public void loadsFromYaml() {
        String yaml =  "inputURI: 'csv://shuttle.csv'\n" +
                "metricColumns:\n" +
                "  - 'd1'\n" +
                "  - 'd2'\n" +
                "labelColumn: 'is_anomaly'\n" +
                "algorithm:\n" +
                "  name: 'iforest'\n" +
                "  parameters:\n" +
                "    treesCount: 100\n" +
                "    param2: 3.14\n";
        assertEquals(configValues, StringObjectMap.fromYaml(yaml).getValues());
    }

    @Test
    public void loadsFromJson() throws IOException {
        String json = "{\"inputURI\": \"csv://shuttle.csv\", \"metricColumns\": [ \"d1\", \"d2\" ], \"labelColumn\": \"is_anomaly\"," +
                "  \"algorithm\": { \"name\": \"iforest\", \"parameters\": { \"treesCount\": 100, \"param2\": 3.14 } } }";
        assertEquals(configValues, StringObjectMap.fromJson(json).getValues());
    }

    @Test
    public void testGet() {
        StringObjectMap config = new StringObjectMap(configValues);

        String labelColumn = config.get("labelColumn");
        String notExisting = config.get("notExisting");
        List<String> columns = config.get("metricColumns");
        StringObjectMap algorithm = config.getMap("algorithm");
        StringObjectMap notExistingObj = config.getMap("notExisting");
        StringObjectMap emptyObj = config.getMapOrEmpty("notExisting");
        StringObjectMap parameters = algorithm.getMap("parameters");
        int treesCount = parameters.get("treesCount");
        double param2 = parameters.get("param2");
        int defaultValue = parameters.get("notExisting", 42);
        int treesCount2 = parameters.get("treesCount", 42);

        assertEquals("is_anomaly", labelColumn);
        assertNull(notExisting);
        assertEquals(Lists.newArrayList("d1", "d2"), columns);
        assertEquals(ImmutableMap.of(
                "name", "iforest",
                "parameters", ImmutableMap.of(
                        "treesCount", 100,
                        "param2", 3.14
                )), algorithm.getValues());
        assertNull(notExistingObj);
        assertEquals(ImmutableMap.of(), emptyObj.getValues());
        assertEquals(100, treesCount);
        assertEquals(3.14, param2, 0.001);
        assertEquals(42, defaultValue);
        assertEquals(treesCount, treesCount2);
    }

    @Test
    public void testMerge() {
        Map<String, Object> parametersValues = new StringObjectMap(configValues).getMap("algorithm").getMap("parameters").getValues();
        StringObjectMap parameters = new StringObjectMap(parametersValues);

        Map<String, Object> newParameters = ImmutableMap.of(
                "treesCount", 42,
                "newParam", 123
        );

        StringObjectMap merged = parameters.merge(newParameters);

        assertEquals(ImmutableMap.of(
                "treesCount", 42,
                "param2", 3.14,
                "newParam", 123
        ), merged.getValues());
        assertEquals(parametersValues, parameters.getValues());
    }
}