package alexp.macrobase.pipeline.benchmark.config;

import alexp.macrobase.ingest.Uri;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;

public class DatasetConfig {
    private final String datasetId;
    private final Uri uri;
    private final String[] metricColumns;
    private final String labelColumn;

    public DatasetConfig(String datasetId, Uri uri, String[] metricColumns, String labelColumn) {
        this.datasetId = datasetId;
        this.uri = uri;
        this.metricColumns = metricColumns;
        this.labelColumn = labelColumn;
    }

    public static DatasetConfig load(StringObjectMap conf) {
        Uri uri = new Uri(conf.get("uri"));
        String id = conf.get("id", uri.getPath());
        //noinspection unchecked
        String[] metricColumns = ((List<String>) conf.get("metricColumns")).toArray(new String[0]);
        String labelColumn = conf.get("labelColumn");

        return new DatasetConfig(id, uri, metricColumns, labelColumn);
    }

    public StringObjectMap toMap() {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        mapBuilder.putAll(ImmutableMap.of(
                "uri", uri.getOriginalString(),
                "id", datasetId,
                "metricColumns", Lists.newArrayList(metricColumns)
                ));
        if (labelColumn != null) {
            mapBuilder.put("labelColumn", labelColumn);
        }
        return new StringObjectMap(mapBuilder.build());
    }

    public String getDatasetId() {
        return datasetId;
    }

    public Uri getUri() {
        return uri;
    }

    public String[] getMetricColumns() {
        return metricColumns;
    }

    public String getLabelColumn() {
        return labelColumn;
    }
}
