package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class AlgorithmConfig {
    private final String algorithmId;
    private final StringObjectMap parameters;

    public AlgorithmConfig(String algorithmId, StringObjectMap parameters) {
        this.algorithmId = algorithmId;
        this.parameters = parameters;
    }

    public static AlgorithmConfig load(StringObjectMap conf) {
        String id = conf.get("id");
        StringObjectMap parameters = conf.getMap("parameters");

        return new AlgorithmConfig(id, parameters);
    }

    public StringObjectMap toMap() {
        return new StringObjectMap(ImmutableMap.of(
                "id", algorithmId,
                "parameters", parameters.getValues()
        ));
    }

    public String getAlgorithmId() {
        return algorithmId;
    }

    public StringObjectMap getParameters() {
        return parameters;
    }
}
