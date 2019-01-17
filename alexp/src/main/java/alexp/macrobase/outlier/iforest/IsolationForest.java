/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 *    IsolationForest.java
 *    Copyright (C) 2012-16 University of Waikato, Hamilton, New Zealand
 *
 *    https://github.com/Waikato/weka-trunk
 *
 */
package alexp.macrobase.outlier.iforest;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.utils.DataFrameUtils;
import alexp.macrobase.utils.MathUtils;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Implements the isolation forest method for anomaly detection.<br>
 * <br>
 * Note that this classifier is designed for anomaly detection, it is not designed for solving two-class or multi-class classification problems!<br>
 * <br>
 * The data is expected to have have a class attribute with one or two values, which is ignored at training time. The distributionForInstance() method returns (1 - anomaly score) as the first element in the distribution, the second element (in the case of two classes) is the anomaly score.<br>
 * <br>
 * To evaluate performance of this method for a dataset where anomalies are known, simply code the anomalies using the class attribute: normal cases should correspond to the first value of the class attribute, anomalies to the second one.<br>
 * <br>
 * For more information, see:<br>
 * <br>
 * Fei Tony Liu, Kai Ming Ting, Zhi-Hua Zhou: Isolation Forest. In: ICDM, 413-422, 2008.
 * <br><br>
 *
 * @author Eibe Frank (eibe@cs.waikato.ac.nz)
 */
public class IsolationForest extends MultiMetricClassifier {

    // The set of trees
    private Tree[] trees = null;

    // The number of trees
    private int treesCount = 100;

    // The subsample size
    private int subsampleSize = 256;

    // training set size, the maximum number of elements that will be used during the training (from the beginning of each batch)
    // can be used to avoid shuffling of all elements (during trees generation) in very big batches
    private int trainSize = 100000;

    private boolean retrainOnEachInput = true;

    // output of Classifier
    private DataFrame output;

    public IsolationForest(String[] columns) {
        super(columns);
    }

    /**
     * Builds the forest.
     */
    public void buildClassifier(List<double[]> instances) throws Exception {
        // Reduce subsample size if data is too small
        if (instances.size() < subsampleSize) {
            subsampleSize = instances.size();
        }

        instances = new ArrayList<>(instances);

        // Generate trees
        trees = new Tree[treesCount];
        Random rand = new Random();
        for (int i = 0; i < treesCount; i++) {
            Collections.shuffle(instances, rand);
            trees[i] = new Tree(instances.subList(0, subsampleSize), rand, 0,
                    (int) Math.ceil(MathUtils.log2(instances.size())));
        }
    }

    /**
     * Returns the average path length of an unsuccessful search. Returns 0 if
     * argument is less than or equal to 1
     */
    private static double c(double n) {
        if (n <= 1.0) {
            return 0;
        }
        return 2 * (Math.log(n - 1) + 0.5772156649) - (2 * (n - 1) / n);
    }

    /**
     * Returns distribution of scores.
     */
    public double score(double[] inst) {
        double avgPathLength = 0;
        for (Tree m_tree : trees) {
            avgPathLength += m_tree.pathLength(inst);
        }
        avgPathLength /= trees.length;

        return Math.pow(2, -avgPathLength / c(subsampleSize));
    }

    @Override
    public void process(DataFrame input) throws Exception {
        List<double[]> inputRows = DataFrameUtils.toRowArray(input, columns);

        if (trees == null || retrainOnEachInput) {
            buildClassifier(inputRows.subList(0, Math.min(trainSize, input.getNumRows())));
        }

        output = input.copy();

        double[] resultColumn = new double[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
            resultColumn[i] = score(inputRows.get(i));
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    public void setTreesCount(int k) {
        treesCount = k;
    }

    public void setSubsampleSize(int n) {
        subsampleSize = n;
    }

    public void setTrainSize(int trainSize) {
        this.trainSize = trainSize;
    }

    public void setRetrainOnEachInput(boolean retrainOnEachInput) {
        this.retrainOnEachInput = retrainOnEachInput;
    }

    /**
     * Inner class for building and using an isolation tree.
     */
    protected class Tree {
        // The size of the node
        private int m_size;

        // The split attribute
        private int m_attributeIndex;

        // The split point
        private double m_splitPoint;

        // The successors
        private Tree[] m_successors;

        /**
         * Constructs a tree from data
         */
        Tree(List<double[]> instances, Random rand, int height, int maxHeight) {
            // Set size of node
            m_size = instances.size();

            // Stop splitting if necessary
            if (instances.size() <= 1 || height == maxHeight) {
                return;
            }

            final int attributesCount = instances.get(0).length;

            // Compute mins and maxs and eligible attributes
            ArrayList<Integer> al = new ArrayList<>();
            double[][] minmax = new double[2][attributesCount];
            for (int j = 0; j < attributesCount; j++) {
                minmax[0][j] = instances.get(0)[j];
                minmax[1][j] = minmax[0][j];
            }
            for (int i = 1; i < instances.size(); i++) {
                double[] inst = instances.get(i);
                for (int j = 0; j < attributesCount; j++) {
                    if (inst[j] < minmax[0][j]) {
                        minmax[0][j] = inst[j];
                    }
                    if (inst[j] > minmax[1][j]) {
                        minmax[1][j] = inst[j];
                    }
                }
            }
            for (int j = 0; j < attributesCount; j++) {
                if (minmax[0][j] < minmax[1][j]) {
                    al.add(j);
                }
            }

            // Check whether any eligible attributes have been found
            if (al.isEmpty()) {
                return;
            }

            // Randomly pick an attribute and split point
            m_attributeIndex = al.get(rand.nextInt(al.size()));
            m_splitPoint = (rand.nextDouble() * (minmax[1][m_attributeIndex] - minmax[0][m_attributeIndex])) + minmax[0][m_attributeIndex];

            // Create sub trees
            m_successors = new Tree[2];
            for (int i = 0; i < 2; i++) {
                ArrayList<double[]> tempData = new ArrayList<>(instances.size());
                for (int j = 0; j < instances.size(); j++) {
                    if ((i == 0) && (instances.get(j)[m_attributeIndex] < m_splitPoint)) {
                        tempData.add(instances.get(j));
                    }
                    if ((i == 1) && (instances.get(j)[m_attributeIndex] >= m_splitPoint)) {
                        tempData.add(instances.get(j));
                    }
                }
                tempData.trimToSize();
                m_successors[i] = new Tree(tempData, rand, height + 1, maxHeight);
            }
        }

        /**
         * Returns path length according to algorithm.
         */
        double pathLength(double[] inst) {
            if (m_successors == null) {
                return c(m_size);
            }
            if (inst[m_attributeIndex] < m_splitPoint) {
                return m_successors[0].pathLength(inst) + 1.0;
            } else {
                return m_successors[1].pathLength(inst) + 1.0;
            }
        }
    }
}
