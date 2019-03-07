package alexp.macrobase.pipeline.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class StringObjectMap {
    private ImmutableMap<String, Object> values;

    public StringObjectMap(Map<String, Object> values) {
        this.values = ImmutableMap.copyOf(values);
    }

    public static StringObjectMap fromYaml(String yamlString) {
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        Map<String, Object> values = yaml.loadAs(yamlString, Map.class);
        return new StringObjectMap(values);
    }

    public static StringObjectMap fromYamlFile(String yamlFilePath) throws IOException {
        return fromYaml(FileUtils.readFileToString(new File(yamlFilePath), StandardCharsets.UTF_8));
    }

    public static StringObjectMap fromJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(
                jsonString,
                new TypeReference<Map<String, Object>>() {}
        );
        return new StringObjectMap(map);
    }

    public static StringObjectMap fromJsonFile(String jsonFilePath) throws IOException {
        return fromJson(FileUtils.readFileToString(new File(jsonFilePath), StandardCharsets.UTF_8));
    }

    public String toYaml() {
        Yaml yaml = new Yaml();
        return yaml.dump(values);
    }

    public void toYamlFile(String filePath) throws IOException {
        FileUtils.writeStringToFile(new File(filePath), toYaml(), StandardCharsets.UTF_8);
    }

    public String toJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(values);
    }

    public void toJsonFile(String filePath) throws IOException {
        FileUtils.writeStringToFile(new File(filePath), toJson(), StandardCharsets.UTF_8);
    }

    public Map<String, Object> getValues() {
        return values;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) values.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, T defaultValue) {
        return (T) values.getOrDefault(key, defaultValue);
    }

    public StringObjectMap getMap(String key) {
        Map<String, Object> values = get(key);
        return values == null ? null : new StringObjectMap(values);
    }

    public StringObjectMap getMapOrEmpty(String key) {
        Map<String, Object> values = get(key);
        return new StringObjectMap(values == null ? new HashMap<>() : values);
    }

    public List<StringObjectMap> getMapList(String key) {
        List<Map<String, Object>> item = get(key);
        if (item == null) {
            return new ArrayList<>();
        }
        return item.stream()
                .map(StringObjectMap::new)
                .collect(Collectors.toList());
    }

    public OptionalDouble getOptionalDouble(String key) {
        Double value = get(key);
        return value == null ? OptionalDouble.empty() : OptionalDouble.of(value);
    }

    public StringObjectMap merge(StringObjectMap other) {
        return merge(other.getValues());
    }

    public StringObjectMap merge(Map<String, Object> other) {
        Map<String, Object> valuesCopy = new HashMap<>(values);
        valuesCopy.putAll(other);
        return new StringObjectMap(valuesCopy);
    }
}
