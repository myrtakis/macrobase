package alexp.macrobase;

import alexp.macrobase.pipeline.benchmark.ClassifierEvaluationPipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Benchmark {
    private final OptionParser optionParser = new OptionParser();
    private final OptionSpec<String> aucOption;
    private final OptionSpec<String> gsOption;

    private Benchmark() {
        aucOption = optionParser.accepts("auc", "Run evaluation (AUC, F1, etc.) of outlier detection algorithms")
                .withRequiredArg().describedAs("config_file_path");
        gsOption = optionParser.accepts("gs", "Run Grid Search for parameters of outlier detection algorithms")
                .withRequiredArg().describedAs("config_file_path");
    }

    private void runAuc(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        ClassifierEvaluationPipeline pipeline = new ClassifierEvaluationPipeline(conf);

        pipeline.run();
    }

    private void runGridSearch(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        ClassifierEvaluationPipeline pipeline = new ClassifierEvaluationPipeline(conf);

        pipeline.runGridSearch();
    }

    private void showUsage() throws IOException {
        optionParser.printHelpOn(System.out);
        System.out.println("Examples:");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml");
        System.out.println("  --gs alexp/data/outlier/gridsearch_config.yaml");
    }

    private int run(String[] args) throws Exception {
        if (args.length == 0) {
            showUsage();
            return 1;
        }

        OptionSet options;
        try {
            options = optionParser.parse(args);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            showUsage();
            return 2;
        }

        if (options.has(aucOption)) {
            String confFilePath = aucOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                System.out.println("Config file not found");
                return 3;
            }

            runAuc(confFilePath);
        }

        if (options.has(gsOption)) {
            String confFilePath = gsOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                System.out.println("Config file not found");
                return 4;
            }

            runGridSearch(confFilePath);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new Benchmark().run(args);
        System.exit(exitCode);
    }
}
