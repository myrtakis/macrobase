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

    public static void main(String[] args) throws Exception {
        final String batchConfFilePath = args.length > 0 ? args[0] : "alexp/demo/batch_xlsx.yaml";
        if (!Files.exists(Paths.get(batchConfFilePath))) {
            System.out.println("Specify batch config file");
            System.exit(1);
        }

        System.out.println("*** Running batch pipeline ***");

        runBatchPipeline(batchConfFilePath);

        final String streamConfFilePath = args.length > 1 ? args[1] : "alexp/demo/stream.yaml";
        if (!Files.exists(Paths.get(streamConfFilePath))) {
            System.out.println("Specify streaming config file");
            System.exit(1);
        }

        System.out.println("*** Running streaming pipeline ***");

        runStreamingPipeline(streamConfFilePath);
    }
}
