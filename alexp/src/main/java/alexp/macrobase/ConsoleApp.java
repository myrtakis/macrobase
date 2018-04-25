package alexp.macrobase;

import alexp.macrobase.pipeline.BatchPipeline;
import alexp.macrobase.pipeline.StreamingPipeline;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsoleApp {
    private static void runBatchPipeline(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        Pipeline pipeline = new BatchPipeline(conf);

        Explanation result = pipeline.results();

        System.out.println(result.prettyPrint());
    }

    private static void runStreamingPipeline(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        StreamingPipeline pipeline = new StreamingPipeline(conf);

        AtomicInteger counter = new AtomicInteger(1);
        pipeline.run(result -> {
            System.out.println("*** Result #" + counter.getAndIncrement());
            System.out.println(result.prettyPrint());
        });
    }

    private static void showUsage() {
        System.out.println("Usage: [--b [batchConfigPath]] [--s [streamConfigPath]]");
        System.out.println("  --b - run batch config using the specified config (default alexp/demo/batch.yaml)");
        System.out.println("  --s - run stream config using the specified config (default alexp/demo/stream.yaml)");
        System.out.println("Examples:");
        System.out.println("  --b my_batch_config.yaml");
        System.out.println("  --s my_stream_config.yaml");
        System.out.println("  --b --s");
    }

    public static void main(String[] args) throws Exception {
        boolean runBatch = false;
        boolean runStream = false;
        String batchConfFilePath = "alexp/demo/batch.yaml";
        String streamConfFilePath = "alexp/demo/stream.yaml";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--b":
                    runBatch = true;
                    if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                        batchConfFilePath = args[i + 1];
                    }
                    break;
                case "--s":
                    runStream = true;
                    if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                        streamConfFilePath = args[i + 1];
                    }
                    break;
            }
        }

        if (!runBatch && !runStream) {
            showUsage();
            return;
        }

        if (runBatch) {
            if (!Files.exists(Paths.get(batchConfFilePath))) {
                System.out.println("Specify batch config file");
                System.exit(1);
            }

            System.out.println("*** Running batch pipeline ***");

            runBatchPipeline(batchConfFilePath);
        }

        if (runStream) {
            if (!Files.exists(Paths.get(streamConfFilePath))) {
                System.out.println("Specify streaming config file");
                System.exit(1);
            }

            System.out.println("*** Running streaming pipeline ***");

            runStreamingPipeline(streamConfFilePath);
        }
    }
}
