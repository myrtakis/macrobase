package alexp.macrobase;

import alexp.macrobase.pipeline.benchmark.ClassifierEvaluationPipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Benchmark {
    private static void run(String confFilePath) throws Exception {
        PipelineConfig conf = PipelineConfig.fromYamlFile(confFilePath);

        ClassifierEvaluationPipeline pipeline = new ClassifierEvaluationPipeline(conf);

        pipeline.run();
    }

    private static void showUsage() {
        System.out.println("Usage: --auc config_file_path");
        System.out.println("  --auc - run ROC AUC evaluation of outlier detection algorithms");
        System.out.println("Examples:");
        System.out.println("  --auc alexp/data/outlier/benchmark_config.yaml");
    }

    public static void main(String[] args) throws Exception {
        boolean auc = false;
        String confFilePath = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--auc":
                    auc = true;
                    if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                        confFilePath = args[i + 1];
                    }
                    break;
            }
        }

        if (!auc && confFilePath.isEmpty()) {
            showUsage();
            return;
        }

        if (!Files.exists(Paths.get(confFilePath))) {
            System.out.println("Config file not found");
            System.exit(1);
        }

        run(confFilePath);
    }
}
