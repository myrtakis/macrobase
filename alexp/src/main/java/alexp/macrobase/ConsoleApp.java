package alexp.macrobase;

import alexp.macrobase.pipeline.BatchPipeline;
import alexp.macrobase.pipeline.StreamingPipeline;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsoleApp {
    private final OptionParser optionParser = new OptionParser();
    private final OptionSpec<String> batchOption;
    private final OptionSpec<String> streamOption;

    private ConsoleApp() {
        batchOption = optionParser.accepts("b", "Run batch pipeline (read all input at once)")
                .withRequiredArg().describedAs("config_file_path");
        streamOption = optionParser.accepts("s", "Run streaming pipeline (read and process input in portions)")
                .withRequiredArg().describedAs("config_file_path");
    }

    private void runBatchPipeline(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        BatchPipeline pipeline = new BatchPipeline(conf);

        Explanation result = pipeline.results();

        System.out.println(result.prettyPrint());
    }

    private void runStreamingPipeline(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        StreamingPipeline pipeline = new StreamingPipeline(conf);

        AtomicInteger counter = new AtomicInteger(1);
        pipeline.run(result -> {
            System.out.println("*** Result #" + counter.getAndIncrement());
            System.out.println(result.prettyPrint());
        });
    }

    private void showUsage() throws IOException {
        optionParser.printHelpOn(System.out);
        System.out.println("Examples:");
        System.out.println("  -b alexp/demo/batch.yaml");
        System.out.println("  -s alexp/demo/stream.yaml");
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

        if (!options.has(batchOption) && !options.has(streamOption)) {
            showUsage();
            return 1;
        }

        if (options.has(batchOption)) {
            String confFilePath = batchOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                System.out.println("Batch pipeline config file not found");
                return 3;
            }

            System.out.println("*** Running batch pipeline ***");

            runBatchPipeline(confFilePath);
        }

        if (options.has(streamOption)) {
            String confFilePath = streamOption.value(options);
            if (!Files.exists(Paths.get(confFilePath))) {
                System.out.println("Streaming pipeline config file not found");
                return 3;
            }

            System.out.println("*** Running streaming pipeline ***");

            runStreamingPipeline(confFilePath);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = new ConsoleApp().run(args);
        System.exit(exitCode);
    }
}
