package alexp.macrobase;

import alexp.macrobase.pipeline.benchmark.ClassifierEvaluationPipeline;
import alexp.macrobase.utils.ConfigUtils;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class Benchmark {
    private final OptionParser optionParser = new OptionParser();
    private final OptionSpec<String> aucOption;
    private final OptionSpec<String> gsOption;
    private final OptionSpec<String> outputOption;
    private final OptionSpec clearOutputOption;
    private final OptionSpec includeInliersOutputOption;
    private final OptionSpec streamOption;
    private final OptionSpec<String> nabOption;

    private String outputDir;
    private String nabOutputDir;
    private boolean includeInliers = false;
    private boolean streaming = false;

    private Benchmark() {
        aucOption = optionParser.accepts("auc", "Run evaluation (AUC, F1, etc.) of outlier detection algorithms")
                .withRequiredArg().describedAs("config_file_path");
        gsOption = optionParser.accepts("gs", "Run Grid Search for parameters of outlier detection algorithms")
                .withRequiredArg().describedAs("config_file_path");
        outputOption = optionParser.acceptsAll(Arrays.asList("save-output", "so"), "Save output (outliers, charts, etc.) to files in the specified dir (alexp/bench_output by default)")
                .withRequiredArg().describedAs("dir_path");
        clearOutputOption = optionParser.acceptsAll(Arrays.asList("clear-output", "co"), "Clear the output dir");
        includeInliersOutputOption = optionParser.acceptsAll(Arrays.asList("include-inliers", "ii"), "Include inliers in the output").availableIf(outputOption);
        streamOption = optionParser.accepts("s", "Run in streaming mode (default batch)");
        nabOption = optionParser.accepts("nab", "Save output in Numenta Anomaly Benchmark format (detection phase) in the specified dir (by default NAB subdir in the output dir)")
                .availableIf(aucOption).withOptionalArg().describedAs("dir_path");
    }

    private void runAuc(String confFilePath) throws Exception {
        PipelineConfig conf = ConfigUtils.loadFromFile(confFilePath);

        ClassifierEvaluationPipeline pipeline = new ClassifierEvaluationPipeline(conf);
        pipeline.setStreaming(streaming);
        pipeline.setOutputDir(outputDir);
        pipeline.setNabOutputDir(nabOutputDir);
        pipeline.setOutputIncludesInliers(includeInliers);

        pipeline.run();
    }

    private void runGridSearch(String confFilePath) throws Exception {
        PipelineConfig conf = ConfigUtils.loadFromFile(confFilePath);

        ClassifierEvaluationPipeline pipeline = new ClassifierEvaluationPipeline(conf);
        pipeline.setStreaming(streaming);
        pipeline.setOutputDir(outputDir);
        pipeline.setOutputIncludesInliers(includeInliers);

        pipeline.runGridSearch();
    }

    private void showUsage() throws IOException {
        optionParser.printHelpOn(System.out);
        System.out.println("Examples:");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml --s");
        System.out.println("  --gs alexp/data/outlier/gridsearch_config.yaml");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml --save-output alexp/output --clear-output");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml --clear-output --nab alexp/output/nab");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml --clear-output --nab");
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

        streaming = options.has(streamOption);

        if (options.has(outputOption)) {
            outputDir = outputOption.value(options);
            includeInliers = options.has(includeInliersOutputOption);
        }

        if (options.has(clearOutputOption)) {
            String dirToClear = StringUtils.isEmpty(outputDir) ? ClassifierEvaluationPipeline.defaultOutputDir() : outputDir;
            if (!StringUtils.isEmpty(dirToClear) && Files.exists(Paths.get(dirToClear))) {
                try {
                    FileUtils.cleanDirectory(new File(dirToClear));
                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }

        if (options.has(aucOption)) {
            String confFilePath = aucOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                System.out.println("Config file not found");
                return 3;
            }

            if (options.has(nabOption)) {
                nabOutputDir = nabOption.value(options);
                if (nabOutputDir == null) {
                    nabOutputDir = (StringUtils.isEmpty(outputDir) ? ClassifierEvaluationPipeline.defaultOutputDir() : outputDir) + "/NAB";
                }
                if (options.has(clearOutputOption) && Files.exists(Paths.get(nabOutputDir))) {
                    try {
                        FileUtils.cleanDirectory(new File(nabOutputDir));
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    }
                }
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
