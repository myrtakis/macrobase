package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

public class AlgorithmConfig {
    private final String algorithmId;
    private final StringObjectMap parameters;
    private final GridSearchConfig gridSearchConfig;

    public AlgorithmConfig(String algorithmId, StringObjectMap parameters, GridSearchConfig gridSearchConfig) {
        this.algorithmId = algorithmId;
        this.parameters = parameters;
        this.gridSearchConfig = gridSearchConfig;
    }

    public AlgorithmConfig(String algorithmId, StringObjectMap parameters) {
        this(algorithmId, parameters, null);
    }

    public static AlgorithmConfig load(StringObjectMap conf) {
        String id = conf.get("id");
        StringObjectMap parameters = conf.getMap("parameters");
        StringObjectMap gs = conf.getMap("gridsearch");

        return new AlgorithmConfig(id, parameters, gs == null ? null : GridSearchConfig.load(gs));
    }

    public StringObjectMap toMap() {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        mapBuilder.putAll(ImmutableMap.of(
                "id", algorithmId,
                "parameters", parameters.getValues()
        ));
        if (gridSearchConfig != null) {
            mapBuilder.put("gridsearch", gridSearchConfig.toMap().getValues());
        }
        return new StringObjectMap(mapBuilder.build());
    }

    public String getAlgorithmId() {
        return algorithmId;
    }

    public StringObjectMap getParameters() {
        return parameters;
    }

    public GridSearchConfig getGridSearchConfig() {
        return gridSearchConfig;
    }
}
