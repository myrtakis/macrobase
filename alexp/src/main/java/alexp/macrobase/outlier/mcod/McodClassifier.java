package alexp.macrobase.outlier.mcod;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.outlier.ParametersAutoTuner;
import alexp.macrobase.outlier.lof.chen.DistanceMeasureService;
import alexp.macrobase.utils.DataFrameUtils;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.lang3.ArrayUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class McodClassifier extends MultiMetricClassifier implements ParametersAutoTuner {
    private double maxDistance;   // R in paper
    private int minNeighborCount; // k in paper
    private int windowSize;       // W in paper
    private int slide;
    private String timeColumnName = "id";
    private int latestWindowCounter = 0;
    private String labelColumnName = "is_anomaly"; // This parameter has to be inherited by the MultiMetricClassifier
    private String orderedWindow = "_WINDOW";
    private String label_minNeighborCount = "minNeighborCount";
    private String label_maxDistance = "maxDistance";
    private String datasetID;       // Define dataset ID, to save the CSV of features per window.

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

        // INITIALIZATION PHASE (ONLY ONCE)
        if (mcod == null) { // init (can't do it in constructor because of the setters. Maybe should make them constructor parameters, but others classifiers don't do it that way)
            List<double[]> inputWindow = DataFrameUtils.toRowArray(input, columns);
            System.out.println("---------------------------");
            System.out.println("PROCESSING WINDOW SIZE: " + inputWindow.size());
            System.out.println("---------------------------");
            tuneParameters(input);
            mcod = new MicroCluster_New(maxDistance, minNeighborCount, windowSize, slide);
            mcod.setAllowDuplicates(allowDuplicates);
        }

        List<double[]> metricColumns = Arrays.stream(columns).map(input::getDoubleColumnByName).collect(Collectors.toList());

        // DETECT ANOMALIES (PER WINDOW)
        double[] timeColumn = input.getDoubleColumnByName(timeColumnName);
        HashMap<Integer, Integer> timeIndexMap = new HashMap<>();
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
        Arrays.fill(resultColumn, 0.0); // 0.0 value is equal to minNeighborCount - minNeighborCount (inlier value)

        for (Data outlier : outliers) {
            final Integer ind = timeIndexMap.get(outlier.arrivalTime());
            if (ind != null) {
                //System.out.println("OUTLIER CRITERION (REASON THAT HAS BEEN OUTLIER) = "+outlier.criterion);
                resultColumn[ind] = minNeighborCount - outlier.criterion; // reverse the scores to be valid
                //resultColumn[ind] = 1.0;
            }
        }
        // UPDATE THE OUTPUT DATA FRAME (PER WINDOW)
        latestWindowCounter++;
        outputBuilder(input, resultColumn);
    }

    private void outputBuilder(DataFrame windowDF, double[] windowScores) {
        double[] windowOrder = new double[windowScores.length];
        for (int i = 0; i < windowOrder.length; i++) {
            windowOrder[i] = latestWindowCounter;
        }
        if (output != null) {
            DataFrame tempDF = new DataFrame();
            // add all feature columns
            for (String column : columns) {
                tempDF.addColumn(column, ArrayUtils.addAll(output.getDoubleColumnByName(column), windowDF.getDoubleColumnByName(column)));
            }
            // add the label (ground truth) column
            tempDF.addColumn(labelColumnName, ArrayUtils.addAll(output.getDoubleColumnByName(labelColumnName), windowDF.getDoubleColumnByName(labelColumnName)));
            // add the scores column
            tempDF.addColumn(outputColumnName, ArrayUtils.addAll(output.getDoubleColumnByName(outputColumnName), windowScores));
            // add the window order column
            tempDF.addColumn(orderedWindow, ArrayUtils.addAll(output.getDoubleColumnByName(orderedWindow), windowOrder));
            output = tempDF.copy();
        } else {
            windowDF.addColumn(outputColumnName, windowScores);
            windowDF.addColumn(orderedWindow, windowOrder);
            output = windowDF.copy();
        }
    }

    @Override
    public DataFrame getResults() {
        System.out.println("Resulting the final output..");
        return output;
    }

    @Override
    public Map<String, Object> tuneParameters(DataFrame trainSet) {
        System.out.println("Parameter tuning parameters");
        List<double[]> items = DataFrameUtils.getDoubleRows(trainSet, columns);
        int half_size = items.size() / 2;

        double random_r = ThreadLocalRandom.current().nextDouble(0.1, 4); // default was the number 4.

        double random_k = ThreadLocalRandom.current().nextInt(1, half_size);

        setMaxDistance(random_r);
        setMinNeighborCount((int) Math.round(random_k));
        Map<String, Object> newConf = new HashMap<>();
        newConf.put("minNeighborCount", minNeighborCount);
        newConf.put("maxDistance", maxDistance);
        System.out.println("    MCOD Auto tune: MinNeighborCount = " + minNeighborCount);
        System.out.println("    MCOD Auto tune: maxDistance = " + maxDistance);
        return newConf;
    }
    /*
    @Override
    public Map<String, Object> tuneParameters(DataFrame trainSet) {
        System.out.println("Parameter tuning parameters");
        List<double[]> items = DataFrameUtils.getDoubleRows(trainSet, columns);
        double[] minDistances = new double[items.size()];
        for (int i = 0; i < items.size(); i++) {

            double[] item = items.get(i);

            double min = items.size(); //double min = Double.MAX_VALUE / items.size();

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

        double random = ThreadLocalRandom.current().nextDouble(0.1, 2); // default was the number 4.

        double r = Arrays.stream(minDistances).max().getAsDouble() * random;
        System.out.println(r);
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
        System.out.println("    MCOD Auto tune: MinNeighborCount = " + minNeighborCount);
        System.out.println("    MCOD Auto tune: maxDistance = " + maxDistance);
        return newConf;
    }

    */

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

    public void setDatasetID(String datasetID) {
        this.datasetID = datasetID;
    }


    @Override
    public DataFrame getModelInfo() {
        DataFrame infoDF = new DataFrame();
        double[] minNeighborCountDF = new double[1];
        minNeighborCountDF[0] = minNeighborCount;
        double[] maxDistanceDF = new double[1];
        maxDistanceDF[0] = maxDistance;
        infoDF.addColumn(label_minNeighborCount, minNeighborCountDF);
        infoDF.addColumn(label_maxDistance, maxDistanceDF);
        return infoDF;
    }

    @Override
    public void setModelInfo(DataFrame infoDF) {
        try {
            String appendSTR = ",";
            String path = "./alexp/output/mcod#" + beautifyDatasetID() + "#model.csv";
            FileWriter csvWriter = new FileWriter(path);
            csvWriter.append(label_minNeighborCount);
            csvWriter.append(appendSTR);
            csvWriter.append(label_maxDistance);
            csvWriter.append("\n");
            double[] neighbors = infoDF.getDoubleColumnByName(label_minNeighborCount);
            double[] distances = infoDF.getDoubleColumnByName(label_maxDistance);
            csvWriter.append(String.valueOf(neighbors[0])).append(appendSTR).append(String.valueOf(distances[0]));
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String beautifyDatasetID() {
        List<String> collapsedID = Arrays.asList(datasetID.split("/"));
        return collapsedID.get(collapsedID.size() - 1).replace(".csv", "");
    }

}
