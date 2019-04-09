package alexp.macrobase.outlier.lof.bkaluza;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.outlier.Trainable;
import alexp.macrobase.utils.DataFrameUtils;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.lang.Math;
import java.util.*;
import java.util.stream.IntStream;


/**
 * Java implementation of Local Outlier Factor algorithm by [Markus M. Breunig](http://www.dbs.ifi.lmu.de/Publikationen/Papers/LOF.pdf).
 * The implementation accepts a collection `double[]` arrays, where each array of doubles corresponds to an instance.
 *
 * @author Bostjan Kaluza
 * @date June 10, 2016
 */
public class LOF extends MultiMetricClassifier implements Trainable {

    public enum Distance {
        ABS_RELATIVE, EUCLIDIAN;
    }

    /**
     * The training instances
     */
    private Collection<double[]> trainInstances;
    private int numAttributes, numInstances;

    /**
     * The distances among instances.
     */
    private double[][] distTable;

    /**
     * Indices of the sorted distance
     */
    private int[][] distSorted;

    /**
     * The minimum values for training instances
     */
    private double[] minTrain;

    /**
     * The maximum values training instances
     */
    private double[] maxTrain;

    private Distance distanceMeasure;

    private DataFrame output;

    private int kNN = 5;

    private int trainSize = 100;

    private boolean retrainOnEachInput = true;

    public LOF(String[] columns, Distance distanceMeasure) {
        super(columns);
        this.distanceMeasure = distanceMeasure;
    }

    @Override
    public void process(DataFrame input) throws Exception {
        List<double[]> inputRows = DataFrameUtils.toRowArray(input, columns);

        if (distTable == null || retrainOnEachInput) {
            train(inputRows);
        }

        output = input.copy();

        double[] resultColumn = new double[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
            resultColumn[i] = score(inputRows.get(i), kNN);
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    @Override
    public void train(DataFrame input) {
        train(DataFrameUtils.toRowArray(input, columns));
    }

    public void train(List<double[]> trainInstances) {
        trainInstances = trainInstances.subList(0, Math.min(trainSize, trainInstances.size()));

        this.trainInstances = trainInstances;

        numInstances = trainInstances.size();

        double[] first = trainInstances.iterator().next();
        numAttributes = first.length;

        // get the bounds for numeric attributes of training instances:
        minTrain = new double[numAttributes];
        maxTrain = new double[numAttributes];

        for (int i = 0; i < numAttributes; i++) {

            minTrain[i] = Double.POSITIVE_INFINITY;
            maxTrain[i] = Double.NEGATIVE_INFINITY;

            for (double[] instance : trainInstances) {

                if (instance[i] < minTrain[i])
                    minTrain[i] = instance[i];

                if (instance[i] > maxTrain[i])
                    maxTrain[i] = instance[i];
            }
        }


        // fill the table with distances among training instances
        distTable = new double[numInstances + 1][numInstances + 1];
        distSorted = new int[numInstances + 1][numInstances + 1];
        for (int i = 0; i < distSorted.length; i++) {
            for (int j = 0; j < distSorted.length; j++) {
                distSorted[i][j] = j;
            }
        }

        int i = 0, j = 0;
        for (double[] instance1 : trainInstances) {
            j = 0;
            for (double[] instance2 : trainInstances) {
                distTable[i][j] = getDistance(instance1, instance2);
                j++;
            }
            if (i == j)
                distTable[i][j] = -1;
            i++;
        }
    }

    /**
     * Returns LOF score for new example.
     */
    public double score(double[] testInstance, int kNN) {

        calcuateDistanceToTest(testInstance);

        return getLofIdx(numInstances, kNN);
    }

    public int getkNN() {
        return kNN;
    }

    public void setkNN(int kNN) {
        this.kNN = kNN;
    }

    public int getTrainSize() {
        return trainSize;
    }

    public void setTrainSize(int trainSize) {
        this.trainSize = trainSize;
    }

    public void setRetrainOnEachInput(boolean retrainOnEachInput) {
        this.retrainOnEachInput = retrainOnEachInput;
    }

    private double getLofIdx(int index, int kNN) {
        // get the number of nearest neighbors for the current test instance:
        int numNN = getNNCount(kNN, index);

        // get LOF for the current test instance:
        double lof = 0.0;
        for (int i = 1; i <= numNN; i++) {
            double lrdi = getLocalReachDensity(kNN, index);
            lof += (lrdi == 0) ? 0 : getLocalReachDensity(kNN, distSorted[index][i]) / lrdi;
        }
        lof /= numNN;

        return lof;
    }

    private void calcuateDistanceToTest(double[] testInstance) {
        // update the table with distances among training instances and the current test instance:
        int i = 0;
        for (double[] trainInstance : trainInstances) {
            distTable[i][numInstances] = getDistance(trainInstance, testInstance);
            distTable[numInstances][i] = distTable[i][numInstances];
            i++;
        }
        distTable[numInstances][numInstances] = -1;

        // sort the distances
        for (i = 0; i < numInstances + 1; i++) {
            sortIndices(distTable[i], distSorted[i]);
        }
    }

    private double getDistance(double[] first, double[] second) {
        // calculate absolute relative distance
        double distance = 0;

        switch (distanceMeasure) {

            case ABS_RELATIVE:
                for (int i = 0; i < this.numAttributes; i++) {
                    distance += Math.abs(first[i] - second[i]) / (maxTrain[i] - minTrain[i]);
                }

            case EUCLIDIAN:
                for (int i = 0; i < this.numAttributes; i++) {
                    distance += Math.pow(first[i] - second[i], 2);
                }
                distance = Math.sqrt(distance);

            default:
                break;
        }

        return distance;
    }

    private double getReachDistance(int kNN, int firstIndex, int secondIndex) {
        // max({distance to k-th nn of second}, distance(first, second))

        double reachDist = distTable[firstIndex][secondIndex];

        int numNN = getNNCount(kNN, secondIndex);

        if (distTable[secondIndex][distSorted[secondIndex][numNN]] > reachDist)
            reachDist = distTable[secondIndex][distSorted[secondIndex][numNN]];

        return reachDist;
    }

    private int getNNCount(int kNN, int instIndex) {
        int numNN = kNN;

        // if there are more neighbors with the same distance, take them too
        for (int i = kNN; i < distTable.length - 1; i++) {
            if (distTable[instIndex][distSorted[instIndex][i]] == distTable[instIndex][distSorted[instIndex][i + 1]])
                numNN++;
            else
                break;
        }

        return numNN;
    }

    private double getLocalReachDensity(int kNN, int instIndex) {
        // get the number of nearest neighbors:
        int numNN = getNNCount(kNN, instIndex);

        double lrd = 0;

        for (int i = 1; i <= numNN; i++) {
            lrd += getReachDistance(kNN, instIndex, distSorted[instIndex][i]);
        }
        lrd = (lrd == 0) ? 0 : numNN / lrd;

        return lrd;
    }

    private void sortIndices(double[] array, int[] indices) {
        IntArrays.quickSort(indices, (i1, i2) -> Double.compare(array[i1], array[i2]));
    }

}
