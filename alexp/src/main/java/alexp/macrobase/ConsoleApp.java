package alexp.macrobase;

import alexp.macrobase.pipeline.BatchPipeline;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConsoleApp {
    private static void runBatchPipeline(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        Pipeline pipeline = new BatchPipeline(conf);

        Explanation result = pipeline.results();

        System.out.println(result.prettyPrint());
    }

    public static void main(String[] args) throws Exception {
        String confFilePath = args.length > 0 ? args[0] : "alexp/demo/batch.yaml";
        if (!Files.exists(Paths.get(confFilePath))) {
            System.out.println("Specify config file");
            System.exit(1);
        }

        runBatchPipeline(confFilePath);
    }
}
