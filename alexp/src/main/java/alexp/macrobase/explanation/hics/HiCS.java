package alexp.macrobase.explanation.hics;

/*
 This file is part of ELKI:
 Environment for Developing KDD-Applications Supported by Index-Structures

 Copyright (C) 2012
 Ludwig-Maximilians-Universität München
 Lehr- und Forschungseinheit für Datenbanksysteme
 ELKI Development Team

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.explanation.hics.statistics.tests.GoodnessOfFitTest;
import alexp.macrobase.explanation.hics.statistics.tests.KolmogorovSmirnovTest;
import alexp.macrobase.explanation.hics.statistics.tests.TestNames;
import alexp.macrobase.explanation.hics.statistics.tests.WelchTTest;
import alexp.macrobase.explanation.utils.datastructures.heap.Heap;
import alexp.macrobase.explanation.utils.datastructures.heap.TopBoundedHeap;
import alexp.macrobase.explanation.utils.RandomFactory;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import com.google.common.primitives.Doubles;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import javafx.util.Pair;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HiCS extends Explanation {

    /**
     * Monte-Carlo iterations.
     */
    private int m;

    /**
     * Alpha threshold.
     */
    private double alpha;

    /**
     * Statistical test to use.
     */
    private GoodnessOfFitTest statTest;

    /**
     * Candidates limit.
     */
    private int cutoff;

    /**
     * Random generator.
     */
    private RandomFactory rnd = new RandomFactory((long)0);

    /**
     * The output DataFrame.
     */
    private DataFrame output;

    public HiCS(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) {
        super(columns, classifierConf, explanationSettings);
    }

    /*
        OVERRIDE FUNCTIONS
     */

    @Override
    public void process(DataFrame input) throws Exception {

        validateParameters();

        output = input.copy();

        List<List<Integer>> subspaceIndex = buildOneDimensionalIndexes(input);
        Set<HiCSSubspace> subspaces = calculateSubspaces(input, subspaceIndex, rnd.getSingleThreadedRandom());

        List<Double> pointCumulativeScores = Doubles.asList(new double[input.getNumRows()]);

        int subspaceCounter = 1;
        System.out.println("\n");
        for(HiCSSubspace hiCSSubspace : subspaces){
            System.out.print("\rMake Detection in: " + hiCSSubspace.toString() + " (" + (subspaceCounter++) + "/"  + subspaces.size() + ")");
            DataFrame tmpDataFrame = new DataFrame();
            String[] subColumns = new String[hiCSSubspace.cardinality()];
            int counter = 0;
            for (int featureId : hiCSSubspace.getFeatures()) {
                tmpDataFrame.addColumn(columns[featureId], input.getDoubleColumn(featureId));
                subColumns[counter++] = columns[featureId];
            }
            Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
            classifier.process(tmpDataFrame);
            DataFrame results = classifier.getResults();
            reportFunction(hiCSSubspace.getFeatures(), results);
            addArrays(pointCumulativeScores, results.getDoubleColumnByName(outputColumnName));
        }
        // Calculate the average score for each point
        for(int i = 0; i < pointCumulativeScores.size(); i++){
            pointCumulativeScores.set(i, pointCumulativeScores.get(i)/subspaces.size());
        }
        output.addColumn(outputColumnName, pointCumulativeScores.stream().mapToDouble(d -> d).toArray());
        System.out.println("\n");
    }

    boolean createFile = true;
    private void reportFunction(List<Integer> subspaceFeatures, DataFrame results) throws IOException {
        File outputFile = new File("alexp/ExplanationOutput/synth050/synth050_001/HicsOutput/hicsInfo.txt");
        if(createFile) {
            outputFile.delete();
            createFile = false;
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile,true));
        String features = subspaceFeatures.toString();
        bw.write(features);
        bw.newLine();
        for(int pointID : getPointsToExplain()){
            bw.write(pointID + ":\t" + results.getRow(pointID).getAs(outputColumnName));
            bw.newLine();
        }
        bw.write("========================================\n");
        bw.newLine();
        bw.close();
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    /**
     * Takes as input two double arrays of same size and add them to a new double array
     * @param cumulArray
     * @param currArr
     * @return The arr1 + arr2
     */
    private void addArrays(List<Double> cumulArray, double[] currArr) {
        if(cumulArray.size() != currArr.length)
            throw new RuntimeException("The two arrays cannot be added due to different sizes. " + cumulArray.size() + " != "+ currArr.length);

        for(int i = 0; i < currArr.length; i++){
            cumulArray.set(i, cumulArray.get(i) + currArr[i]);
        }
    }

    /**
     * Calculates "index structures" for every attribute, i.e. sorts a
     * List of Strings of every point id in the dataset for every dimension and
     * stores them in a list
     *
     * @param input Dataframe to index
     * @return List of sorted point IDs
     */

    private List<List<Integer>> buildOneDimensionalIndexes(DataFrame input) {
        final int dim = getDatasetDimensionality();
        System.out.println("Dimensionality: " + dim + "\n");
        List<List<Integer>> subspaceIndex = new ArrayList<>();
        for(int i = 0; i < dim; i++){
            double[] values = input.getDoubleColumn(i);
            subspaceIndex.add(getSortedIndices(values));
        }
        return subspaceIndex;
    }

    private List<Integer> getSortedIndices(double[] values) {
        List<Pair<Integer, Double>> listSortedByVal = new ArrayList<>();
        int counter = 0;
        for(double val : values)
            listSortedByVal.add(new Pair<>(counter++,val));
        listSortedByVal.sort(Comparator.comparing(Pair::getValue));
        return listSortedByVal.stream().map(Pair::getKey).collect(Collectors.toList());
    }

    /**
     * Identifies high contrast subspaces in a given full-dimensional database.
     *
     * @param input the relation the hics should be evaluated for
     * @param subspaceIndex Subspace indexes
     * @return a set of high contrast subspaces
     */
    private Set<HiCSSubspace> calculateSubspaces(DataFrame input, List<List<Integer>> subspaceIndex, Random rand) {
        final int dim = getDatasetDimensionality();

        SortedSet<HiCSSubspace> subspaceList = Collections.synchronizedSortedSet(new TreeSet<>(HiCSSubspace.SORT_BY_SUBSPACE));
        TopBoundedHeap<HiCSSubspace> dDimensionalList = new TopBoundedHeap<>(cutoff, HiCSSubspace.SORT_BY_CONTRAST_ASC);

        // compute two-element sets of subspaces
        for(int i = 0; i < dim; i++) {
            for(int j = i + 1; j < dim; j++) {
                HiCSSubspace ts = new HiCSSubspace();
                ts.set(i);
                ts.set(j);
                calculateContrast(input, ts, subspaceIndex, rand);
                dDimensionalList.add(ts);
                System.out.print("\rCalculate Contrast in: " + ts);
            }
        }

        for(int d = 3; !dDimensionalList.isEmpty(); d++) {

            // result now contains all d-dimensional sets of subspaces
            ArrayList<HiCSSubspace> candidateList = new ArrayList<>(dDimensionalList.size());
            for(Heap<HiCSSubspace>.UnorderedIter it = dDimensionalList.unorderedIter(); it.valid(); it.advance()) {
                subspaceList.add(it.get());
                candidateList.add(it.get());
            }

            dDimensionalList.clear();
            // candidateList now contains the *m* best d-dimensional sets
            candidateList.sort(HiCSSubspace.SORT_BY_SUBSPACE);

            for(int i = 0; i < candidateList.size() - 1; i++) {
                for(int j = i + 1; j < candidateList.size(); j++) {
                    HiCSSubspace set1 = candidateList.get(i);
                    HiCSSubspace set2 = candidateList.get(j);

                    HiCSSubspace joinedSet = new HiCSSubspace();
                    joinedSet.or(set1);
                    joinedSet.or(set2);
                    if(joinedSet.cardinality() != d) {
                        continue;
                    }

                    calculateContrast(input, joinedSet, subspaceIndex, rand);
                    System.out.print("\r" + joinedSet);
                    dDimensionalList.add(joinedSet);
                }
            }
            for(HiCSSubspace cand : candidateList) {
                for(Heap<HiCSSubspace>.UnorderedIter it = dDimensionalList.unorderedIter(); it.valid(); it.advance()) {
                    if(it.get().contrast > cand.contrast) {
                        subspaceList.remove(cand);
                        break;
                    }
                }
            }
        }   // end for

        return subspaceList;
    }

    /**
     * Calculates the actual contrast of a given subspace.
     *
     * @param input Dataframe to process
     * @param subspace Subspace
     * @param subspaceIndex Subspace indexes
     */
    private void calculateContrast(DataFrame input, HiCSSubspace subspace, List<List<Integer>> subspaceIndex, Random rand) {
        final int card = subspace.cardinality();
        final double alpha1 = Math.pow(alpha, (1.0 / card));
        final int windowsize = (int) (input.getNumRows() * alpha1);

        double deviationSum = 0.0;

        for(int i = 0; i < m; i++) {        // m is the Monte Carlo iterations
            // Choose a random set bit.
            int chosen = -1;
            for (int tmp = rand.nextInt(card); tmp >= 0; tmp--) {
                chosen = subspace.nextSetBit(chosen + 1);
            }

            // initialize sample
            int totalPointIDs = input.getNumRows() - 1;
            List<Integer> conditionalSample = IntStream.rangeClosed(0, totalPointIDs).boxed().collect(Collectors.toList());

            for(int j = subspace.nextSetBit(0); j >= 0; j = subspace.nextSetBit(j + 1)) {
                if (j == chosen) {
                    continue;
                }

                List<Integer> sortedIndices = subspaceIndex.get(j);
                List<Integer> indexBlock = new ArrayList<>(windowsize);

                // initialize index block
                int seekedPosition = rand.nextInt(input.getNumRows() - windowsize);
                for(int k = 0; k < windowsize; k++){
                    indexBlock.add(sortedIndices.get(seekedPosition + k));
                }

                conditionalSample = getIntersection(conditionalSample, indexBlock);
            }

            if(conditionalSample.size() < 10){
                i--;
                continue;
            }


            // Project conditional set
            double[] sampleValues = new double[conditionalSample.size()];
            {
                int l = 0;
                for(int sampleId : conditionalSample){
                    sampleValues[l] = input.getRow(sampleId).getAs(chosen);
                    l++;
                }
            }
            // Project full set
            double[] fullValues = new double[input.getNumRows()];
            {
                int l = 0;
                for(int sampleId : subspaceIndex.get(chosen)){
                    fullValues[l] = input.getRow(sampleId).getAs(chosen);
                    l++;
                }
            }

            double contrast = statTest.deviation(fullValues, sampleValues);
            if(Double.isNaN(contrast)) {
                i--;
                System.out.println("Contrast was NaN");
                continue;
            }
            deviationSum += contrast;

        } // end for

        subspace.contrast = deviationSum / m;
    }

    /**
     * Calculates the intersection between two lists
     *
     * @param list1
     * @param list2
     * @return the list with the intersection of list1 and list2
     */
    private <T> List<T> getIntersection(List<T> list1, List<T> list2){
        HashSet<T> hashSet1 = new HashSet<>(list1);
        HashSet<T> hashSet2 = new HashSet<>(list2);
        hashSet1.retainAll(hashSet2); // Now hashSet1 contains only the objects appeared in both hash sets
        return new ArrayList<>(hashSet1);
    }

    /*
        SETTER FUNCTIONS
     */

    public void setM(int m) {
        this.m = m;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public void setStatTest(String statTestStr) {
        if(statTestStr.equals(TestNames.KOLMOGOROV_SMIRNOV_TEST.toString()))
            this.statTest = new KolmogorovSmirnovTest();
        else if(statTestStr.equals(TestNames.WELTCH_TTEST.toString()))
            this.statTest = new WelchTTest();
        else
            throw new RuntimeException("Statistical test " + statTestStr + " not found in " + Arrays.asList(TestNames.values()));
    }

    public void setCutoff(int cutoff) {
        this.cutoff = cutoff;
    }

    /*
        VARIANTS
     */

    private void validateParameters() {
        if(alpha < 0 || alpha > 1)
            throw new IllegalArgumentException("alpha parameter must be 0 <= a <= 1");
        if(m <= 0)
            throw new IllegalArgumentException("m parameter must be > 0");
        if(cutoff <= 0)
            throw new IllegalArgumentException("cutoff parameter must be > 0");
    }

     /*
        OTHER FUNCTIONS
     */

    private void analyzeHiCSdatasets(DataFrame input) throws Exception {
        String outputFilePath = "alexp/data/explanation/HiCS_Dataset_Analysis/test_010_000.txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputFilePath)));
        for(Object pointIdObj : getPointsToExplain()){
            int pointId = pointIdObj instanceof Integer ? (Integer) pointIdObj : ((Double) pointIdObj).intValue();
            String subspaces = input.getStringColumnByName("relevant_subspace")[pointId];
            String pointScoreStr = "Point " + pointId + "\n";
            for(List<Integer> subspace : getSubspaces(subspaces)) {
                DataFrame dataFrame = new DataFrame();
                String[] subColumns = new String[subspace.size()];
                int counter = 0;
                for (int featureId : subspace) {
                    dataFrame.addColumn(columns[featureId], input.getDoubleColumn(featureId));
                    subColumns[counter++] = columns[featureId];
                }
                Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
                classifier.process(dataFrame);
                DataFrame results = classifier.getResults();
                pointScoreStr += subspace + "\n" + results.getRow(pointId).getVals().get(subspace.size()) + "\n";
            }
            bw.write(pointScoreStr);
            bw.write("---------\n");
            bw.newLine();
        }
        bw.close();
    }

    private List<List<Integer>> getSubspaces(String subspacesStr) {
        String[] subspacesParts = subspacesStr.split("[\\[\\]]");
        List<List<Integer>> subspaces = new ArrayList<>();
        for(String s : subspacesParts) {
            if (s.contains("-"))
                subspaces.add(Arrays.stream(s.split("[-]")).map(Integer::valueOf).collect(Collectors.toList()));
        }
        return subspaces;
    }



    /**
     * BitSet that holds a contrast value as field. Used for the representation of
     * a subspace in hics
     */
    public static class HiCSSubspace extends BitSet {
        /**
         * Serial version.
         */
        private static final long serialVersionUID = 1L;

        /**
         * The hics contrast value.
         */
        protected double contrast;

        /**
         * Constructor.
         */
        public HiCSSubspace() {
            super();
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("[contrast=").append(contrast);
            for(int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
                buf.append(' ').append(i);
            }
            buf.append(']');
            return buf.toString();
        }

        /**
         * Get the features of a subspace
         */
        public List<Integer> getFeatures() {
            List<Integer> features = new ArrayList<>();
            for(int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)){
                features.add(i);
            }
            return features;
        }

        /**
         * Sort subspaces by their actual subspace.
         */
        public static final Comparator<HiCSSubspace> SORT_BY_CONTRAST_ASC = new Comparator<HiCSSubspace>() {
            @Override
            public int compare(HiCSSubspace o1, HiCSSubspace o2) {
                if(o1.contrast == o2.contrast) {
                    return 0;
                }
                return o1.contrast > o2.contrast ? 1 : -1;
            }
        };

        /**
         * Sort subspaces by their actual subspace.
         */
        public static final Comparator<HiCSSubspace> SORT_BY_CONTRAST_DESC = new Comparator<HiCSSubspace>() {
            @Override
            public int compare(HiCSSubspace o1, HiCSSubspace o2) {
                if(o1.contrast == o2.contrast) {
                    return 0;
                }
                return o1.contrast < o2.contrast ? 1 : -1;
            }
        };

        /**
         * Sort subspaces by their actual subspace.
         */
        public static final Comparator<HiCSSubspace> SORT_BY_SUBSPACE = new Comparator<HiCSSubspace>() {
            @Override
            public int compare(HiCSSubspace o1, HiCSSubspace o2) {
                int dim1 = o1.nextSetBit(0);
                int dim2 = o2.nextSetBit(0);
                while(dim1 >= 0 && dim2 >= 0) {
                    if(dim1 < dim2) {
                        return -1;
                    }
                    else if(dim1 > dim2) {
                        return 1;
                    }
                    dim1 = o1.nextSetBit(dim1 + 1);
                    dim2 = o2.nextSetBit(dim2 + 1);
                }
                return 0;
            }
        };
    }

}
