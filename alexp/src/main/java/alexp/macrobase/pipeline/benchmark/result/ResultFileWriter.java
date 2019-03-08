package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.utils.DataFrameUtils;
import com.google.common.collect.ImmutableMap;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.io.FilenameUtils;

import java.nio.file.Paths;

public class ResultFileWriter implements ResultWriter {
    private String outputDir;

    @Override
    public void write(DataFrame outputData, ExecutionResult result) throws Exception {
        String csvPath = Paths.get(outputDir, result.getBenchmarkConfig().getDatasetConfig().getDatasetId().split("/")).toString();

        DataFrameUtils.saveToCsv(csvPath, outputData);

        result.toMap().merge(ImmutableMap.of(
                "result", result.toMap().getMap("result").merge(ImmutableMap.of(
                        "algorithmOutputFilePath", csvPath
                ))
        )).toJsonFile(FilenameUtils.removeExtension(csvPath) + ".json");
    }

    public String getOutputDir() {
        return outputDir;
    }

    public ResultFileWriter setOutputDir(String outputDir) {
        this.outputDir = outputDir;
        return this;
    }
}
