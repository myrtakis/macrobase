package alexp.macrobase.utils;

import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;

import java.util.*;
import java.util.stream.Collectors;

public class ConfigUtils {
    public static List<PipelineConfig> getObjectsList(PipelineConfig config, String key) {
        List<Map<String, Object>> item = config.get(key);
        if (item == null) {
            return new ArrayList<>();
        }
        return item.stream()
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

    public static PipelineConfig getObj(PipelineConfig conf, String key) {
        Map<String, Object> values = conf.get(key);
        return values == null ? null : new PipelineConfig(values);
    }

    public static PipelineConfig getObjOrEmpty(PipelineConfig conf, String key) {
        Map<String, Object> values = conf.get(key);
        return new PipelineConfig(values == null ? new HashMap<>() : values);
    }

    public static OptionalDouble getOptionalDouble(PipelineConfig conf, String key) {
        Double value = conf.get(key);
        return value == null ? OptionalDouble.empty() : OptionalDouble.of(value);
    }

    public static void addToAllConfigs(List<PipelineConfig> configs, String key, Object val) {
        addToAll(configs.stream().map(PipelineConfig::getValues).collect(Collectors.toList()), key, val);
    }

    public static void addToAll(List<Map<String, Object>> configs, String key, Object val) {
        configs.forEach(conf -> conf.put(key, val));
    }

    public static PipelineConfig merge(Map<String, Object> srcConf, Map<String, Object> conf) {
        Map<String, Object> currConf = new HashMap<>(srcConf);
        currConf.putAll(conf);
        return new PipelineConfig(currConf);
    }

    public static PipelineConfig merge(PipelineConfig srcConf, PipelineConfig conf) {
        return merge(srcConf.getValues(), conf.getValues());
    }

    public static PipelineConfig merge(Map<String, Object> srcConf, PipelineConfig conf) {
        return merge(srcConf, conf.getValues());
    }

    public static PipelineConfig merge(PipelineConfig srcConf, Map<String, Object> conf) {
        return merge(srcConf.getValues(), conf);
    }

    public static PipelineConfig loadFromFile(String confFilePath) throws Exception {
        return PipelineConfig.fromYamlFile(confFilePath);
    }
}
