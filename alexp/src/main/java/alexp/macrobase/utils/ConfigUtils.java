package alexp.macrobase.utils;

import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.stream.Collectors;

public class ConfigUtils {
    public static <T> Set<T> getAllValues(List<StringObjectMap> maps, String key) {
        return maps.stream()
                .map(conf -> conf.<List<T>>get(key))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    public static  List<StringObjectMap> addToAllConfigs(List<StringObjectMap> configs, String key, Object val) {
        if (key == null || val == null)
            return configs;
        return configs.stream()
                .map(c -> c.merge(ImmutableMap.of(key, val)))
                .collect(Collectors.toList());
    }
}
