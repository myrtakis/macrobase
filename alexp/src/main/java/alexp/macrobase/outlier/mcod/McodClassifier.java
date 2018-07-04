package alexp.macrobase.outlier.mcod;

import alexp.macrobase.outlier.MultiMetricClassifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class McodClassifier extends MultiMetricClassifier {
    private double maxDistance = 1; // R in paper
    private int minNeighborCount = 30; // k in paper
    private int windowSize = 1000; // W in paper
    private int slide = 500;
    private String timeColumnName = "id";

    private boolean allowDuplicates = false;

    private MicroCluster_New mcod;

    private DataFrame output;

    public McodClassifier(String columnName) {
        super(columnName);
    }

    public McodClassifier(String[] columns) {
        super(columns);
    }

    @Override
    public void process(DataFrame input) throws Exception {
        if (mcod == null) { // init (can't do it in constructor because of the setters. Maybe should make them constructor parameters, but others classifiers don't do it that way)
            mcod = new MicroCluster_New(maxDistance, minNeighborCount, windowSize, slide);
            mcod.setAllowDuplicates(allowDuplicates);
        }

        List<double[]> metricColumns = Arrays.stream(columns).map(input::getDoubleColumnByName).collect(Collectors.toList());
        double[] timeColumn = input.getDoubleColumnByName(timeColumnName);

        HashMap<Integer, Integer> timeIndexMap = new HashMap<>(); // TODO: make time double in MCOD?

        output = input.copy();

        ArrayList<Data> mcodData = new ArrayList<>();
        double[][] metricRows = new double[input.getNumRows()][columns.length];
        for (int i = 0; i < input.getNumRows(); i++) {
            for (int j = 0; j < columns.length; j++) {
                metricRows[i][j] = metricColumns.get(j)[i];
            }
            mcodData.add(new Data((int) timeColumn[i], metricRows[i]));
            timeIndexMap.put((int) timeColumn[i], i);
        }
        ArrayList<Data> outliers = mcod.detectOutlier(mcodData, (int) timeColumn[timeColumn.length - 1]);

        double[] resultColumn = new double[input.getNumRows()];
        Arrays.fill(resultColumn, 0.0);
        for (Data outlier : outliers) {
            final Integer ind = timeIndexMap.get(outlier.arrivalTime());
            if (ind != null) {
                resultColumn[ind] = 1.0;
            }
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

    public String getTimeColumnName() {
        return timeColumnName;
    }

    public void setTimeColumnName(String timeColumnName) {
        this.timeColumnName = timeColumnName;
    }

    public boolean isAllowDuplicates() {
        return allowDuplicates;
    }

    public void setAllowDuplicates(boolean allowDuplicates) {
        this.allowDuplicates = allowDuplicates;
    }
}
