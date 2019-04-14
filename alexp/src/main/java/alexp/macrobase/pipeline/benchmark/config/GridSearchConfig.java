package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class GridSearchConfig {
    private final String measure;
    private final Map<String, Object[]> parameters; // maybe should store as ArrayList? To avoid conversions, etc. For now using Object[] because it is used in GridSearch

    public GridSearchConfig(String measure, Map<String, Object[]> parameters) {
        this.measure = measure;
        this.parameters = parameters;
    }

    public static GridSearchConfig load(StringObjectMap conf) {
        String measure = conf.get("measure", "pr");
        StringObjectMap parameters = conf.getMap("parameters");

        return new GridSearchConfig(measure, parameters.getValues().entrySet().stream()
                .collect(Collectors.toMap(o -> o.getKey(), o -> ((ArrayList) o.getValue()).toArray())));
    }

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "measure", measure,
                "parameters", parameters.entrySet().stream()
                        .collect(Collectors.toMap(o -> o.getKey(), o -> Arrays.asList(o.getValue())))
        ));
    }

    public String getMeasure() {
        return measure;
    }

    public Map<String, Object[]> getParameters() {
        return parameters;
    }
}
