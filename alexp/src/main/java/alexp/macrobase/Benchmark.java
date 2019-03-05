package alexp.macrobase;

import alexp.macrobase.pipeline.benchmark.LegacyClassifierEvaluationPipeline;
import alexp.macrobase.pipeline.config.StringObjectMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class Benchmark {
    PrintStream out = System.out;
    PrintStream err = System.err;

    private final OptionParser optionParser = new OptionParser();
    private final OptionSpec<String> benchmarkOption;
    private final OptionSpec<String> aucOption;
    private final OptionSpec<String> gsOption;
    private final OptionSpec<String> outputOption;
    private final OptionSpec clearOutputOption;
    private final OptionSpec includeInliersOutputOption;
    private final OptionSpec streamOption;
    private final OptionSpec<String> nabOption;

    private final OptionSpec<String> drawDataPlotsOption;

    private String outputDir;
    private String nabOutputDir;
    private boolean includeInliers = false;
    private boolean streaming = false;

    Benchmark() {
        benchmarkOption = optionParser.acceptsAll(Arrays.asList("benchmark", "b"), "Run benchmark for outlier detection algorithms. Outputs time and raw scores.")
                .withRequiredArg().describedAs("config_file_path");

        aucOption = optionParser.accepts("auc", "Run simple quality evaluation of outlier detection algorithms." +
                "Original version of the benchmark, has more features (NAB, streaming, ...) and produces more output (AUC, F1, plots, etc.) but less structured output format than -b.")
                .availableUnless(benchmarkOption).withRequiredArg().describedAs("config_file_path");

        gsOption = optionParser.accepts("gs", "Run Grid Search for parameters of outlier detection algorithms")
                .availableUnless(benchmarkOption, aucOption).withRequiredArg().describedAs("config_file_path");

        outputOption = optionParser.acceptsAll(Arrays.asList("save-output", "so"), "Save output (outliers, charts, etc.) to files in the specified dir (alexp/bench_output by default)")
                .withRequiredArg().describedAs("dir_path");
        clearOutputOption = optionParser.acceptsAll(Arrays.asList("clear-output", "co"), "Clear the output dir");

        includeInliersOutputOption = optionParser.acceptsAll(Arrays.asList("include-inliers", "ii"), "Include inliers in the output (only for --auc)")
                .availableIf(outputOption).availableUnless(benchmarkOption);

        streamOption = optionParser.accepts("s", "Run in streaming mode (default batch)")
                .availableUnless(benchmarkOption);

        nabOption = optionParser.accepts("nab", "Save output in Numenta Anomaly Benchmark format (detection phase) in the specified dir (by default NAB subdir in the output dir)")
                .availableIf(aucOption).withOptionalArg().describedAs("dir_path");

        drawDataPlotsOption = optionParser.accepts("draw-plots", "Draw plots (normal and anomalous points) for 1D datasets")
                .withRequiredArg().describedAs("config_file_path");
    }

    private void runAuc(String confFilePath) throws Exception {
        StringObjectMap conf = StringObjectMap.fromYamlFile(confFilePath);

        LegacyClassifierEvaluationPipeline pipeline = new LegacyClassifierEvaluationPipeline(conf);
        pipeline.setStreaming(streaming);
        pipeline.setOutputDir(outputDir);
        pipeline.setNabOutputDir(nabOutputDir);
        pipeline.setOutputIncludesInliers(includeInliers);
        pipeline.setOutputStream(out);

        pipeline.run();
    }

    private void runGridSearch(String confFilePath) throws Exception {
        StringObjectMap conf = StringObjectMap.fromYamlFile(confFilePath);

        LegacyClassifierEvaluationPipeline pipeline = new LegacyClassifierEvaluationPipeline(conf);
        pipeline.setStreaming(streaming);
        pipeline.setOutputDir(outputDir);
        pipeline.setOutputIncludesInliers(includeInliers);
        pipeline.setOutputStream(out);

        pipeline.runGridSearch();
    }

    private void drawPlots(String confFilePath) throws Exception {
        StringObjectMap conf = StringObjectMap.fromYamlFile(confFilePath);

        LegacyClassifierEvaluationPipeline pipeline = new LegacyClassifierEvaluationPipeline(conf);
        pipeline.setStreaming(streaming);
        pipeline.setOutputDir(outputDir);
        pipeline.setOutputStream(out);

        pipeline.drawPlots();
    }

    private void showUsage() throws IOException {
        optionParser.printHelpOn(out);
        out.println("Examples:");
        out.println("  -b alexp/data/outlier/config.yaml");
        out.println("  --auc alexp/data/outlier/benchmark_config.yaml");
        out.println("  --auc alexp/data/outlier/benchmark_config.yaml --s");
        out.println("  --gs alexp/data/outlier/gridsearch_config.yaml");
        out.println("  --auc alexp/data/outlier/benchmark_config.yaml --save-output alexp/output --clear-output");
        out.println("  --auc alexp/data/outlier/benchmark_config.yaml --clear-output --nab alexp/output/nab");
        out.println("  --auc alexp/data/outlier/benchmark_config.yaml --clear-output --nab");
        out.println("  --draw-plots alexp/data/outlier/s5_plots_config.yaml --so alexp/output --co");
    }

    int run(String[] args) throws Exception {
        if (args.length == 0) {
            err.println("Not enough parameters");
            showUsage();
            return 1;
        }

        OptionSet options;
        try {
            options = optionParser.parse(args);
        } catch (Exception ex) {
            err.println(ex.getMessage());
            showUsage();
            return 2;
        }

        if (!options.has(benchmarkOption) && !options.has(aucOption) && !options.has(gsOption) && !options.has(drawDataPlotsOption)) {
            err.println("Not enough parameters");
            showUsage();
            return 1;
        }

        streaming = options.has(streamOption);

        if (options.has(outputOption)) {
            outputDir = outputOption.value(options);
            includeInliers = options.has(includeInliersOutputOption);
        }

        if (options.has(clearOutputOption)) {
            String dirToClear = StringUtils.isEmpty(outputDir) ? LegacyClassifierEvaluationPipeline.defaultOutputDir() : outputDir;
            if (!StringUtils.isEmpty(dirToClear) && Files.exists(Paths.get(dirToClear))) {
                try {
                    FileUtils.cleanDirectory(new File(dirToClear));
                } catch (IOException ex) {
                    err.println(ex.getMessage());
                }
            }
        }

        if (options.has(aucOption)) {
            String confFilePath = aucOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                err.println("Config file not found");
                return 3;
            }

            if (options.has(nabOption)) {
                nabOutputDir = nabOption.value(options);
                if (nabOutputDir == null) {
                    nabOutputDir = (StringUtils.isEmpty(outputDir) ? LegacyClassifierEvaluationPipeline.defaultOutputDir() : outputDir) + "/NAB";
                }
                if (options.has(clearOutputOption) && Files.exists(Paths.get(nabOutputDir))) {
                    try {
                        FileUtils.cleanDirectory(new File(nabOutputDir));
                    } catch (IOException ex) {
                        err.println(ex.getMessage());
                    }
                }
            }

            runAuc(confFilePath);
        }

        if (options.has(benchmarkOption)) {
            out.println("NOT IMPLEMENTED");
            return 42;
        }

        if (options.has(gsOption)) {
            String confFilePath = gsOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                err.println("Config file not found");
                return 3;
            }

            runGridSearch(confFilePath);
        }

        if (options.has(drawDataPlotsOption)) {
            String confFilePath = drawDataPlotsOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                err.println("Config file not found");
                return 3;
            }

            drawPlots(confFilePath);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new Benchmark().run(args);
        System.exit(exitCode);
    }
}
