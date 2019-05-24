package alexp.macrobase.pipeline.benchmark.result;

import alexp.macrobase.pipeline.benchmark.config.ExecutionType;
import alexp.macrobase.utils.DataFrameUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.io.FilenameUtils;
import java.nio.file.Paths;
import java.util.Objects;

public class ResultFileWriter implements ResultWriter {
    private String outputDir;
    private String baseFileName;
    private ExecutionType executionType;

    public ResultFileWriter(ExecutionType executionType) {
        this.executionType = executionType;
    }

    @Override
    public void write(DataFrame outputData, ExecutionResult result) throws Exception {
        String[] pathParts = Strings.isNullOrEmpty(result.getClassifierId()) ?
                new String[]{baseFileName} :
                Lists.newArrayList(
                        FilenameUtils.getBaseName(result.getExecutionConfig().getDatasetConfig().getDatasetId()),
                        typeToDirName(executionType),
                        result.getExplainerId(),
                        result.getClassifierId(),
                        baseFileName)
                        .stream()
                        .filter(Objects::nonNull)
                        .toArray(String[]::new);

        String baseFilePath = Paths.get(outputDir, pathParts).toString();

        String csvPath = Paths.get(outputDir, baseFileName).toString();

        DataFrameUtils.saveToCsv(baseFilePath + ".csv", outputData);

        result.toMap().merge(ImmutableMap.of(
                "result", result.toMap().getMap("result").merge(ImmutableMap.of(
                        "algorithmOutputFilePath", FilenameUtils.getName(csvPath)
                )).getValues()
        )).toJsonFile(baseFilePath + ".json");
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

    private static String typeToDirName(ExecutionType type) {
        switch (type) {
            case BATCH_CLASSIFICATION:
            case STREAMING_CLASSIFICATION:
                return "classification";
            case EXPLANATION:
                return "explanation";
        }
        throw new IllegalArgumentException();
    }
}
