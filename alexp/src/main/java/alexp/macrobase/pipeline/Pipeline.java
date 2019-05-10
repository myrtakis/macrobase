package alexp.macrobase.pipeline;

import alexp.macrobase.explanation.Itemset;
import alexp.macrobase.utils.DataFrameUtils;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public abstract class Pipeline {
    protected long lastGeneratedTime = -1;

    private String outputDir = defaultOutputDir();

    private boolean outputIncludesInliers = false;

    private StringBuilder infoText = new StringBuilder();

    protected PrintStream out = System.out;

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public static String defaultOutputDir() {
        String currentDirName = Paths.get(".").toAbsolutePath().normalize().getFileName().toString();
        return currentDirName.equals("alexp") ? "output" : "alexp/output";
    }

    public boolean isOutputIncludesInliers() {
        return outputIncludesInliers;
    }

    public void setOutputIncludesInliers(boolean outputIncludesInliers) {
        this.outputIncludesInliers = outputIncludesInliers;
    }

    public void setOutputStream(PrintStream out) {
        this.out = out;
    }

    protected void createAutoGeneratedColumns(DataFrame dataFrame, String... columns) {
        Arrays.stream(columns).filter(Pipelines::isAutoGeneratedColumn).forEach(column -> {
            switch (column) {
                case "__autogenerated_time": {
                    Pipelines.generateTimeColumn(dataFrame, column, lastGeneratedTime + 1);
                    double[] values = dataFrame.getDoubleColumnByName(column);
                    lastGeneratedTime = (long) values[values.length - 1];
                }
                break;
                default:
                    throw new RuntimeException("Unknown column " + column);
            }
        });
    }

    protected void saveData(String dir, String baseFileName, DataFrame data) throws IOException {
        DataFrameUtils.saveToCsv(Paths.get(dir, baseFileName + ".csv").toString(), data);
    }

    protected void saveData(String baseFileName, DataFrame data) throws IOException {
        saveData(getOutputDir(), baseFileName, data);
    }

    protected void saveOutliers(String baseFileName, DataFrame data, String outlierOutputColumn) throws IOException {
        if (!outputIncludesInliers) {
            data = data.filter(outlierOutputColumn, this::isOutlier);
        }

        saveData(baseFileName, data);
    }

    protected void saveExplanation(String baseFileName, DataFrame data, String outlierOutputColumn, Explanation explanation) throws IOException, MacroBaseException {
        List<Itemset> itemsets = Pipelines.getItemsets(explanation);

        if (!outputIncludesInliers) {
            data = data.filter(outlierOutputColumn, this::isOutlier);
        }

        for (int i = 0; i < itemsets.size(); i++) {
            Itemset itemset = itemsets.get(i);

            DataFrame result = DataFrameUtils.filterByAll(data, itemset.getAttributes());

            saveData(baseFileName + "_group" + (i + 1), result);
        }
    }

    private boolean isOutlier(double value) {
        return value > 0.0;
    }

    protected void printInfo() {
        printInfo("");
    }

    protected void printInfo(Object obj) {
        printInfo(obj.toString());
    }

    // TODO: may be good to add multiple buffers, to allow clearing only some part of the output (for saving to the next file) and leave "global" like dataset info, etc.
    protected void printInfo(String line) {
        out.println(line);

        infoText.append(line).append(System.lineSeparator());
    }

    protected void saveInfo(String basefileName) throws IOException {
        String filePath = Paths.get(getOutputDir(), basefileName + ".txt").toString();
        FileUtils.write(new File(filePath), infoText.toString(), "utf-8");

        infoText = new StringBuilder();
    }
}
