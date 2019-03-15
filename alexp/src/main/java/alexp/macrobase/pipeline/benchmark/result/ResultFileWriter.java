package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.utils.DataFrameUtils;
import com.google.common.collect.ImmutableMap;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.nio.file.Paths;

public class ResultFileWriter implements ResultWriter {
    private String outputDir;
    private String baseFileName;

    @Override
    public void write(DataFrame outputData, ExecutionResult result) throws Exception {
        String csvPath = Paths.get(outputDir, baseFileName + ".csv").toString();

        DataFrameUtils.saveToCsv(csvPath, outputData);

        result.toMap().merge(ImmutableMap.of(
                "result", result.toMap().getMap("result").merge(ImmutableMap.of(
                        "algorithmOutputFilePath", csvPath
                ))
        )).toJsonFile(Paths.get(outputDir, baseFileName + ".json").toString());
    }

    public String getOutputDir() {
        return outputDir;
    }

    public ResultFileWriter setOutputDir(String outputDir) {
        this.outputDir = outputDir;
        return this;
    }

    public String getBaseFileName() {
        return baseFileName;
    }

    public ResultFileWriter setBaseFileName(String baseFileName) {
        this.baseFileName = baseFileName;
        return this;
    }
}
