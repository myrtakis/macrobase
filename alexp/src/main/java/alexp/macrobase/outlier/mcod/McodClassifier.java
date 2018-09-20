package alexp.macrobase.outlier.mcod;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.outlier.ParametersAutoTuner;
import alexp.macrobase.outlier.lof.chen.DistanceMeasureService;
import alexp.macrobase.utils.DataFrameUtils;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;
import java.util.stream.Collectors;

public class McodClassifier extends MultiMetricClassifier implements ParametersAutoTuner {
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

    @Override
    public Map<String, Object> tuneParameters(DataFrame trainSet) {
        List<double[]> items = DataFrameUtils.getDoubleRows(trainSet, columns);

        double[] minDistances = new double[items.size()];

        for (int i = 0; i < items.size(); i++) {
            double[] item = items.get(i);
            double min = Double.MAX_VALUE / items.size();
            for (int j = 0; j < items.size(); j++) {
                if (i == j) {
                    continue;
                }
                double[] otherItem = items.get(j);

                double dist = DistanceMeasureService.euclideanDistance(item, otherItem);
                if (dist <= 0.00000001) {
                    continue;
                }
                if (dist < min) {
                    min = dist;
                }
            }
            minDistances[i] = min;
        }

        double r = Arrays.stream(minDistances).max().getAsDouble() * 4;

        int[] neighborsCounts = new int[items.size()];
        Arrays.fill(neighborsCounts, 0);

        for (int i = 0; i < items.size(); i++) {
            double[] item = items.get(i);
            for (int j = 0; j < items.size(); j++) {
                if (i == j) {
                    continue;
                }
                double[] otherItem = items.get(j);
                if (item == otherItem) {
                    continue;
                }

                double dist = DistanceMeasureService.euclideanDistance(item, otherItem);
                if (dist < r) {
                    neighborsCounts[i]++;
                }
            }
        }

        double k = Arrays.stream(neighborsCounts).min().getAsInt();

        setMaxDistance(r);
        setMinNeighborCount((int) Math.round(k));

        Map<String, Object> newConf = new HashMap<>();
        newConf.put("minNeighborCount", minNeighborCount);
        newConf.put("maxDistance", maxDistance);
        return newConf;
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
