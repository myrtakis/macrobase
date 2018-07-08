package alexp.macrobase.utils;

import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;

import java.util.*;
import java.util.stream.Collectors;

public class ConfigUtils {
    public static List<PipelineConfig> getObjectsList(PipelineConfig config, String key) {
        return config.<List<Map<String, Object>>>get(key).stream()
                .map(PipelineConfig::new)
                .collect(Collectors.toList());
    }

    public static Set<String> getAllValues(List<PipelineConfig> configs, String key) {
        return configs.stream()
                .map(conf -> conf.<List<String>>get(key))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    public static void addToAllConfigs(List<PipelineConfig> configs, String key, Object val) {
        addToAll(configs.stream().map(PipelineConfig::getValues).collect(Collectors.toList()), key, val);
    }

    public static void addToAll(List<Map<String, Object>> configs, String key, Object val) {
        configs.forEach(conf -> conf.put(key, val));
    }
}
