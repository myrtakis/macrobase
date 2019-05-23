package alexp.macrobase.explanation.utils.analytics.config;

import alexp.macrobase.ingest.Uri;
import alexp.macrobase.pipeline.benchmark.config.SettingsConfig;
import alexp.macrobase.pipeline.config.StringObjectMap;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnalyticsConfig {

    private final String                        outputColumn;
    private final String                        relSubspaceColumn;
    private final List<Pair<String, String>>    explainers;
    private final SettingsConfig                settingsConfig;

    private static final String outputColumnTag        =   "outputColumn";
    private static final String relSubspaceColumnTag   =   "relSubspaceColumn";
    private static final String explainersTag          =   "explainers";
    private static final String explainerIdTag         =   "id";
    private static final String outputFilePathTag      =   "outputFilePath";
    private static final String settingsConfigTag      =   "settingsConfigPath";


    public AnalyticsConfig(String outputColumn, String relSubspaceColumn,
                           List<Pair<String, String>> explainers, SettingsConfig settingsConfig) {
        this.outputColumn = outputColumn;
        this.relSubspaceColumn = relSubspaceColumn;
        this.explainers = explainers;
        this.settingsConfig = settingsConfig;
    }

    public static AnalyticsConfig load(StringObjectMap conf) throws IOException {
        return new AnalyticsConfig(
                getOutputColumn(conf),
                getRelSubspaceColumn(conf),
                getExplainers(conf),
                getSettings(conf)
        );
    }

    private static String getOutputColumn(StringObjectMap conf) {
        return conf.get(outputColumnTag);
    }

    private static String getRelSubspaceColumn(StringObjectMap conf) {
        return conf.get(relSubspaceColumnTag);
    }

    private static List<Pair<String, String>> getExplainers(StringObjectMap conf) {
        List<StringObjectMap> explainersConf = conf.getMapList(explainersTag);
        List<Pair<String,String>> explainersList = new ArrayList<>();
        for(StringObjectMap e : explainersConf) {
            explainersList.add(new Pair<>(e.get(explainerIdTag), e.get(outputFilePathTag)));
        }
        return explainersList;
    }

    private static SettingsConfig getSettings(StringObjectMap conf) throws IOException {
        return SettingsConfig.load(conf.get(settingsConfigTag));
    }

    public String getOutputColumn() {
        return outputColumn;
    }

    public String getRelSubspaceColumn() {
        return relSubspaceColumn;
    }

    public List<Pair<String, String>> getExplainers() {
        return explainers;
    }

    public SettingsConfig getSettingsConfig() {
        return settingsConfig;
    }
}
