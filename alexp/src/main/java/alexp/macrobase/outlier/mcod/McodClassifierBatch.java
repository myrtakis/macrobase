package alexp.macrobase.outlier.mcod;

import alexp.macrobase.outlier.MultiMetricClassifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class McodClassifierBatch extends MultiMetricClassifier {
    private double maxDistance = 1; // R in paper
    private int minNeighborCount = 30; // k in paper
    private int windowSize = 1000; // W in paper
    private int slide = 500;

    private DataFrame output;

    public McodClassifierBatch(String columnName) {
        super(columnName);
    }

    public McodClassifierBatch(String[] columns) {
        super(columns);
    }

    @Override
    public void process(DataFrame input) throws Exception {
        List<double[]> metricColumns = Arrays.stream(columns).map(input::getDoubleColumnByName).collect(Collectors.toList());

        output = input.copy();

        ArrayList<Data> mcodData = new ArrayList<>();
        double[][] metricRows = new double[input.getNumRows()][columns.length];
        for (int i = 0; i < input.getNumRows(); i++) {
            for (int j = 0; j < columns.length; j++) {
                metricRows[i][j] = metricColumns.get(j)[i];
            }
            mcodData.add(new Data(i, metricRows[i]));
        }

        MicroCluster_New mcod = new MicroCluster_New(maxDistance, minNeighborCount, windowSize, slide);
        ArrayList<Data> outliers = mcod.detectOutlier(mcodData, 0);

        double[] resultColumn = new double[input.getNumRows()];
        Arrays.fill(resultColumn, 0.0);
        for (Data outlier : outliers) {
            resultColumn[outlier.arrivalTime()] = 1.0;
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    public double getMaxDistance() {
        return maxDistance;
    }

    public void setMaxDistance(double maxDistance) {
        this.maxDistance = maxDistance;
    }

    public int getMinNeighborCount() {
        return minNeighborCount;
    }

    public void setMinNeighborCount(int minNeighborCount) {
        this.minNeighborCount = minNeighborCount;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getSlide() {
        return slide;
    }

    public void setSlide(int slide) {
        this.slide = slide;
    }
}
