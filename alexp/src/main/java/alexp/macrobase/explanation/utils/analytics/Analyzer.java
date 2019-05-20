package alexp.macrobase.explanation.utils.analytics;

import alexp.macrobase.explanation.utils.analytics.config.AnalyticsConfig;
import alexp.macrobase.explanation.utils.analytics.operator.Operator;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Analyzer {

    public static void main(String[] args) throws Exception{
        run(args[0]);
    }

    private static void run(String confPath) throws Exception {
        List<String> confFilePaths = Lists.newArrayList(confPath);
        if (Files.isDirectory(Paths.get(confPath))) {
            confFilePaths = Files.list(Paths.get(confPath))
                    .map(Path::toString)
                    .filter(s -> s.endsWith("yaml"))
                    .collect(Collectors.toList());
        }
        for(String c : confFilePaths){
            System.out.println("Run configuration file " + c);
            AnalyticsConfig analyticsConfig = AnalyticsConfig.load(StringObjectMap.fromYamlFile(c));
            Operator operator = new Operator(analyticsConfig);
            operator.operate(false);
        }
    }

}
