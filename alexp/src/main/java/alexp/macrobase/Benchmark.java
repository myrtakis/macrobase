package alexp.macrobase;

import alexp.macrobase.pipeline.benchmark.result.ResultFileWriter;
import alexp.macrobase.pipeline.config.StringObjectMap;
import com.google.common.collect.Lists;
import alexp.macrobase.pipeline.MacroPipeline;
import alexp.macrobase.pipeline.benchmark.config.BenchmarkConfig;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Benchmark {
    PrintStream out = System.out;
    PrintStream err = System.err;

    private final OptionParser optionParser = new OptionParser();
    private final OptionSpec<String> benchmarkOption;
    private final OptionSpec<String> outputOption;
    private final OptionSpec clearOutputOption;
    private final OptionSpec includeInliersOutputOption;
    private final OptionSpec streamOption;
    private final OptionSpec explanationOption;
    private final OptionSpec<String> dataDirOption;
    private BenchmarkConfig configuration;

    private OptionSet options;

    Benchmark() {
        benchmarkOption = optionParser.acceptsAll(Arrays.asList("benchmark", "b"), "Run benchmark for outlier detection algorithms. Outputs time and raw scores.")
                .withRequiredArg().describedAs("config_file_path");
        outputOption = optionParser.acceptsAll(Arrays.asList("save-output", "so"), "Save output (outliers, charts, etc.) to files in the specified dir (alexp/bench_output by default)")
                .withRequiredArg().describedAs("dir_path");
        clearOutputOption = optionParser.acceptsAll(Arrays.asList("clear-output", "co"), "Clear the output dir");
        includeInliersOutputOption = optionParser.acceptsAll(Arrays.asList("include-inliers", "ii"), "Include inliers in the output (only for --auc)")
                .availableIf(outputOption).availableUnless(benchmarkOption);
        streamOption = optionParser.accepts("s", "Run in streaming mode (default batch)")
                .availableIf(benchmarkOption);
        explanationOption = optionParser.accepts("e", "Run in explanation mode (default non explanation)")
                .availableIf(benchmarkOption);
        dataDirOption = optionParser.accepts("data-dir", "Path of the root data dir that will be prepended for paths from the config file")
                .availableIf(benchmarkOption).withRequiredArg().describedAs("root_dir_path");
    }

    private void runBenchmark(String confFilePath) throws Exception {

        configuration = BenchmarkConfig.load(StringObjectMap.fromYamlFile(confFilePath));

        MacroPipeline pipeline = new MacroPipeline(configuration, dataDirOption.value(options),
                new ResultFileWriter()
                        .setOutputDir(outputOption.value(options))
                        .setBaseFileName(FilenameUtils.getBaseName(confFilePath)));

        if (options.has(streamOption)) {
            pipeline.streamingMode();
        } else if (options.has(explanationOption)) {
            pipeline.explanationMode();
        } else {
            pipeline.classiciationMode();
        }

    }

    private void showUsage() throws IOException {
        optionParser.printHelpOn(out);
        out.println("Examples:");
        out.println("  -b alexp/data/outlier/config.yaml");
        out.println("  -b alexp/data/outlier/config.yaml --so alexp/myoutput --co");
        out.println("  -b alexp/data/outlier/stream_config.yaml --s");
        out.println("  -b alexp/data/outlier/explanation_config.yaml --e");
    }

    int run(String[] args) throws Exception {

        // VALIDATE THE GIVEN OPTIONS
        validateObtainOptions(args);

        if (options.has(clearOutputOption)) {
            try {
                if (outputOption.value(options) == null) {
                    FileUtils.cleanDirectory(new File("null"));
                } else {
                    FileUtils.cleanDirectory(new File(outputOption.value(options)));
                }
            } catch (IOException ex) {
                err.println(ex.getMessage());
            }
        }

        if (options.has(benchmarkOption)) {

            // THE YAML CONFIGURATION OF BENCHMARK OPTION (MUST BE INCLUDED)
            String confPath = benchmarkOption.value(options);
            if (!Files.exists(Paths.get(confPath))) {
                err.println("Config file not found");
                return 3;
            }

            List<String> confFilePaths = Lists.newArrayList(confPath);

            // A LIST OF THE YAML CONFIGURATION PATHS OF A GIVEN DIRECTORY
            if (Files.isDirectory(Paths.get(confPath))) {
                out.println("Running for all configs in " + confPath);
                out.println("This should not be used for time/memory measurements");
                out.println();
                confFilePaths = Files.list(Paths.get(confPath))
                        .map(Path::toString)
                        .filter(s -> s.endsWith("yaml"))
                        .collect(Collectors.toList());
            }

            // ITERATE OVER ALL CONFIGURATION PATHS
            for (String confFilePath : confFilePaths) {
                runBenchmark(confFilePath);
            }
        }

        return 0;
    }


    private void validateObtainOptions(String[] args) throws IOException {
        // ERROR: NOT ENOUGH PARAMETERS
        if (args.length == 0) {
            err.println("Not enough parameters");
            showUsage();
            throw new IllegalStateException("args.length == 0");
        }
        // FETCH ALL USER DEFINED OPTIONS
        try {
            options = optionParser.parse(args);
        } catch (Exception ex) {
            err.println(ex.getMessage());
            showUsage();
            throw new IllegalStateException("parse error");
        }
        // ERROR: NOT ENOUGH PARAMETERS
        if (!options.has(benchmarkOption)) {
            err.println("Not enough parameters");
            showUsage();
            throw new IllegalStateException("!options.has(benchmarkOption) && !options.has(gsOption)");
        }
    }


    public static void main(String[] args) throws Exception {
        int exitCode = new Benchmark().run(args);
        System.exit(exitCode);
    }
}
