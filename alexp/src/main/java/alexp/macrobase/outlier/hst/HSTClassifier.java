package alexp.macrobase.outlier.hst;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.outlier.Trainable;
import alexp.macrobase.outlier.Updatable;
import alexp.macrobase.utils.DataFrameUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import javafx.util.Pair;
import org.apache.commons.lang3.ArrayUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
// last checked: 15/07/2019 - Report: All (debugging and correctness) tests passed!

public class HSTClassifier extends MultiMetricClassifier implements Trainable, Updatable {

    // Constant Parameters:
    private String wMaxAtt = "maxAtt";
    private String wMinAtt = "minAtt";
    private String traverseLeft = "leftIndexes";
    private String traverseRight = "rightIndexes";
    private String labelColumnName = "is_anomaly"; // This parameter has to be inherited by the MultiMetricClassifier
    private String orderedWindow = "_WINDOW";
    private String treeTag = "_TREE";
    private String featuresTag = "_FEATURES";
    private String loggerWindowForestFeaturesPath = "./null/hst#windowForestFeatures.csv";
    private double inlierScore = -1.0;

    // Node controlling variables:
    private boolean trainable = true;
    private boolean processable = false;
    private boolean updatable = false;
    private List<double[]> referenceWindow = new ArrayList<>();
    private int trainSize = 0;
    private int latestWindowCounter = 0;

    // Hyper parameters:
    private int numTree;            // Number of Half Space Trees
    private int numSub;             // Sub sample size
    private int numDim;             // Sub dimensional size
    private int depthLimit;         // Maximum depth limitation
    private int forgetThreshold;    // Threshold (bottom) to apply the forget mechanism
    private double contamination;   // contamination (rate of outliers)

    // Model and Output:
    private List<Node> forest = new ArrayList<>();
    private DataFrame output;
    private Multimap<Integer, List<List<Integer>>> pWindowForestFeatures = ArrayListMultimap.create();

    // Setters & Getters:
    public void setNumTree(int numTree) {
        this.numTree = numTree;
    }

    public void setNumSub(int numSub) {
        this.numSub = numSub;
    }

    public void setNumDim(int numDim) {
        this.numDim = numDim;
    }

    public void setDepthLimit(int depthLimit) {
        this.depthLimit = depthLimit;
    }

    public void setForgetThreshold(int forgetThreshold) {
        this.forgetThreshold = forgetThreshold;
    }

    public void setContamination(double contamination) {
        this.contamination = ((contamination <= 1.0 && contamination >= 0.0) ? contamination : 1.0);
    }

    // Constructor
    public HSTClassifier(String[] columns) {
        super(columns);
    }

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                  TRAINING PHASE
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public void train(DataFrame input) {
        if (trainable) {
            // Increment by the size of real input data (inliers + outliers)
            trainSize += (DataFrameUtils.toRowArray(input, columns)).size();
            // Add only the inliers into the reference window
            referenceWindow.addAll((DataFrameUtils.toRowArray(removeOutliers(input), columns)));
            if (trainSize >= numSub) { // The size of ref window must be at least numTree * numSub.
                System.out.println("Reference window size (inliers only): " + referenceWindow.size() + " (inliers + outliers): " + trainSize);
                //System.out.println("Reference Window Size: " + referenceWindowSize);
                buildClassifier(referenceWindow); // train an Node forest.
                //System.out.println("==========================================");
                //System.out.println("============== INITIAL TREE ==============");
                //System.out.println("==========================================");
                //printTree(forest.get(0));
                //System.out.println("==========================================");
                trainable = false; // the Node is not any more trainable.
                referenceWindow.clear(); // flush the reference window from the memory.
            }
        }
    }

    private void buildClassifier(List<double[]> window) {
        int numInst = window.size();        // number of data points.
        int dimInst = window.get(0).length; // number of dimensions.
        // BUILD A MODEL (FOREST)
        for (int i = 0; i < numTree; i++) {
            List<Integer> indexSub; // indexes of the selected (sub) samples.
            List<Integer> indexDim; // indexes of the selected (sub) dimensions.
            // Sampling with replacement: samples (data points).
            if (numSub < numInst && numSub > 0) {
                indexSub = subSampling(numInst, numSub, true);
            } else {
                indexSub = subSampling(numInst, numSub, false);
            }
            // Sampling with replacement: features
            if (numDim < dimInst && numDim > 0) {
                indexDim = subSampling(dimInst, numDim, true);
            } else {
                indexDim = subSampling(dimInst, numDim, false);
            }
            // Calculate the work space of the current (sub) window.
            Map<String, double[]> workspace = workSpaceHST(subInstances(window, indexSub));
            // Construct a single Node.
            Node constructedNode = treeBuilder(new Node(), window, workspace.get(wMaxAtt), workspace.get(wMinAtt),
                    indexSub, 0, indexDim);
            // Append the constructed tree to the model.
            forest.add(constructedNode);
        }
    }

    private Node treeBuilder(Node tree, List<double[]> window, double[] maxAtt, double[] minAtt, List<Integer> indexSub, int curtDepth, List<Integer> indexDim) {
        tree.depth = curtDepth;
        tree.size = indexSub.size();
        tree.ageStatus = (tree.size > 0) ? 1 : 0;
        // INTERNAL NODE
        if (curtDepth < depthLimit) {
            tree.nodeStatus = 1;
            tree.splitAttribute = indexDim.get(new Random().nextInt(((indexDim.size() - 1)) + 1)); // randomly select a dimension q
            tree.splitPoint = (maxAtt[tree.splitAttribute] + minAtt[tree.splitAttribute]) / (double) 2;  // split by the mid value of the selected dimension q
            Map<String, List<Integer>> guide = treeTraverse(window, indexSub, tree.splitAttribute, tree.splitPoint);
            // Build the left side of the sub tree
            double oldMaxAtt = maxAtt[tree.splitAttribute];
            maxAtt[tree.splitAttribute] = tree.splitPoint; // re-define the upper bound when moving left (<-)
            tree.leftChild = treeBuilder(new Node(), window, maxAtt, minAtt, guide.get(traverseLeft), curtDepth + 1, indexDim);
            // Build the right side of the sub tree
            maxAtt[tree.splitAttribute] = oldMaxAtt;
            minAtt[tree.splitAttribute] = tree.splitPoint; // re-define the lower bound when moving right (->)
            tree.rightChild = treeBuilder(new Node(), window, maxAtt, minAtt, guide.get(traverseRight), curtDepth + 1, indexDim);
        }
        // EXTERNAL NODE
        else {
            tree.nodeStatus = 0;
            tree.splitAttribute = Integer.MIN_VALUE;
            tree.splitPoint = Double.MIN_VALUE;
            tree.leftChild = null;
            tree.rightChild = null;
        }
        return tree;
    }

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                PREDICTION PHASE
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public void process(DataFrame input) {
        List<double[]> inputWindow = (DataFrameUtils.toRowArray(input, columns)); // normalize the feature columns per window
        if (!trainable) {
            if (processable) {
                latestWindowCounter++;
                double[] scores = anomalyDetection(inputWindow);
                outputBuilder(input, scores);
                System.out.println("Processing window size: " + inputWindow.size());

                // CREATE A MAP OF THE CURRENT WINDOW -> TREES -> FEATURES
                List<List<Integer>> forestFeatures = new ArrayList<>();
                for (Node node : forest) {
                    List<Integer> treeFeatures = new ArrayList<>();
                    readTreeFeatures(node, treeFeatures);
                    forestFeatures.add(treeFeatures);
                }
                pWindowForestFeatures.put(latestWindowCounter, forestFeatures);

            } else {
                processable = true;
            }
        }
    }

    private double[] anomalyDetection(List<double[]> window) {
        // number of data points in the current window
        int numInst = window.size();
        List<Integer> windowIndexes = new ArrayList<>();
        for (int i = 0; i < numInst; i++) {
            windowIndexes.add(i);
        }
        // Estimated mass of each tree for every data point.
        List<double[]> windowForestScores = new ArrayList<>();
        for (Node tree : forest) {
            double[] windowTreeScores = treeMassEstimation(window, windowIndexes, tree, new double[numInst]);
            windowForestScores.add(windowTreeScores);
        }
        // TOP-K scores are considered as candidate outliers.
        if (contamination < 1.0) {
            return scoreContaminator(scoreAccumulator(windowForestScores));
        } else {
            return scoreAccumulator(windowForestScores);
        }
    }

    private double[] treeMassEstimation(List<double[]> window, List<Integer> indexesDP, Node tree, double[] massTree) {
        // - - - - - - - - - - - - - - - - - - - - - - //
        int status = tree.nodeStatus;
        double size = tree.size;
        double depth = tree.depth;
        int splitFeature = tree.splitAttribute;
        double splitValue = tree.splitPoint;
        Node leftChild = tree.leftChild;
        Node rightChild = tree.rightChild;
        // - - - - - - - - - - - - - - - - - - - - - - //
        // LEAF NODE
        if (status == 0) {
            for (int cur : indexesDP) {
                massTree[cur] = score(size, depth);
            }
        }
        // INTERNAL NODE
        else {
            Map<String, List<Integer>> guide = treeTraverse(window, indexesDP, splitFeature, splitValue);
            treeMassEstimation(window, guide.get(traverseLeft), leftChild, massTree);
            treeMassEstimation(window, guide.get(traverseRight), rightChild, massTree);
        }
        return massTree;
    }

    private double[] scoreContaminator(double[] scores) {
        // select the TOP-K data points (indices)
        int k = (int) Math.ceil((contamination * scores.length));
        int topK = ((k > 1) ? k : 1);
        // rank in ascending order the data points accompanied with their scores, by scores
        List<Pair<Integer, Double>> rankedSC = new ArrayList<>();
        int counter = 0;
        for (double score : scores) {
            rankedSC.add(new Pair<>(counter++, score));
        }
        rankedSC.sort(Comparator.comparing(Pair::getValue)); //e.g. [(3, 100), (2,200), (1, 300)] -> (id, score)
        // destroy the non-contaminated scores
        for (int rs = 0; rs < rankedSC.size(); rs++) {
            if (rs >= topK) {
                scores[rankedSC.get(rs).getKey()] = inlierScore; // inlier score (reminder: you can never have negative mass)
            }
        }
        return scores;
    }

    private double[] scoreAccumulator(List<double[]> massForest) {
        int massTrees = massForest.size();
        int instances = massForest.get(0).length;
        double[] scores = new double[instances];
        for (int i = 0; i < instances; i++) { // for each data point (instance)
            for (int t = 0; t < massTrees; t++) {  // for each HS-Tree
                scores[i] += massForest.get(t)[i]; // final score for i is the "SUM OF THE SCORES OBTAINED FROM EACH HS-TREE" in the ensemble.
            }
        }
        return scores;
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

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                  UPDATE PHASE
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public void update(DataFrame input) {
        List<double[]> inputWindow = (DataFrameUtils.toRowArray(input, columns)); // normalize the feature columns per window
        if (!trainable) {
            if (updatable) {
                massUpdate(inputWindow); // Update the mass profiles using the current data points.
                //System.out.println("==========================================");
                //System.out.println("============== UPDATED TREE ==============");
                //System.out.println("==========================================");
                //printTree(forest.get(0));
                //System.out.println("==========================================");
            } else {
                updatable = true;
            }
        }
    }

    private void massUpdate(List<double[]> window) {
        // number of data points in the current window
        int numInst = window.size();
        List<Integer> windowIndexes = new ArrayList<>();
        for (int i = 0; i < numInst; i++) {
            windowIndexes.add(i);
        }
        for (Node tree : forest) {
            // [PRE-UPDATE] SET ALL NODES AS OLD NODES.
            treeAgeResetBeforeUpdate(tree);
            // [UPDATE] INCREMENT MASS PROFILES AND SET THE YOUNG NODES
            treeMassUpdate(window, windowIndexes, tree);
            // [UPDATE] DECREMENT THE MASS PROFILES OF K (UNIFORMLY SELECTED) OLD LEAVES
            if (tree.size > forgetThreshold && forgetThreshold > 0) {
                treeMassForget(tree, numInst);
            }
        }
    }

    private void treeAgeResetBeforeUpdate(Node node) {
        node.ageStatus = 0;
        if (node.nodeStatus == 1) {
            treeAgeResetBeforeUpdate(node.leftChild);
            treeAgeResetBeforeUpdate(node.rightChild);
        }
    }

    private void treeMassUpdate(List<double[]> window, List<Integer> indexesDP, Node node) {
        // - - - - - - - - - - - - - - - - - - - - - - //
        int status = node.nodeStatus;
        int splitFeature = node.splitAttribute;
        double splitValue = node.splitPoint;
        Node leftChild = node.leftChild;
        Node rightChild = node.rightChild;
        // - - - - - - - - - - - - - - - - - - - - - - //
        if (indexesDP.size() > 0) {
            node.ageStatus = 1;            // set as young node by reference
            node.size += indexesDP.size(); // update mass (r) profile by reference
        }
        // INTERNAL NODE
        if (status == 1) {
            Map<String, List<Integer>> guide = treeTraverse(window, indexesDP, splitFeature, splitValue);
            treeMassUpdate(window, guide.get(traverseLeft), leftChild);
            treeMassUpdate(window, guide.get(traverseRight), rightChild);
        }
    }

    private void treeMassForget(Node tree, int k) {
        // update the external (leaf) nodes mass profiles, according to the forgetting mechanism
        forgetENMP(tree, k);
        // synchronize the internal nodes mass profiles, according to the new leaf node mass profiles
        synchronizeINMP(tree);
    }

    private void forgetENMP(Node tree, int k) {
        List<Node> oldestLeaves = findOldestLeaves(tree, new ArrayList<>());
        while (k > 0) {
            int ruNumber = ThreadLocalRandom.current().nextInt(0, oldestLeaves.size());
            Node selectedOldNode = oldestLeaves.get(ruNumber);
            // Decrement by one the non-zero mass profile of the selected old leaf (by reference)
            if (selectedOldNode.size > 0) {
                selectedOldNode.size -= 1;
            }
            k -= 1;
        }
    }

    private double synchronizeINMP(Node node) {
        if (node.nodeStatus == 1) {
            node.size = synchronizeINMP(node.rightChild) + synchronizeINMP(node.leftChild);
        }
        return node.size;
    }

    private List<Node> findOldestLeaves(Node node, List<Node> nodes) {
        if (node.nodeStatus == 1) {
            findOldestLeaves(node.leftChild, nodes);
            findOldestLeaves(node.rightChild, nodes);
        } else {
            if (node.ageStatus == 0) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                FINAL RESULT OUTPUT
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public DataFrame getResults() {
        System.out.println("Resulting the final output..");
        if (output != null) {
            scoreReverser(); // reverse the scores of the data frame
            if (contamination < 1.0) {
                contaminatedDF(); // keep only the contaminated data points.
            }
        }
        /*
        System.out.println("======================== DATA POINTS ===========================");
        double[] data1 = output.getDoubleColumnByName("d1");
        for (int i = 0; i < data1.length; i++) {
            System.out.println(data1[i]);
        }
        System.out.println("======================= ANOMALY SCORES ========================");
        double[] results = output.getDoubleColumnByName(outputColumnName);
        for (double r : results) {
            System.out.println(r);
        }
        System.out.println("===============================================================");
        */

        // EXPORT THE WINDOW/TREES/FEATURES INTO A CSV
        try {
            logger_tree_features_csv();
            System.out.println("[Logger] The Window/Trees/Features information has been exported at "+loggerWindowForestFeaturesPath+"...");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return output;
    }

    private void scoreReverser() {
        DataFrame tempDF = new DataFrame();
        // Calculate the reversed scores
        double[] scores = output.getDoubleColumnByName(outputColumnName);
        double[] scoresReversed = new double[scores.length];
        double maxScore = scores[0];
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] > maxScore) {
                maxScore = scores[i];
            }
        }
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] != inlierScore) {
                scoresReversed[i] = (maxScore - scores[i]);
            } else {
                scoresReversed[i] = scores[i];
            }
            //System.out.println("THE REVERSED SCORE IS : "+scoresReversed[i]);
        }
        // add all feature columns
        for (String column : columns) {
            tempDF.addColumn(column, output.getDoubleColumnByName(column));
        }
        // add the label (ground truth) column
        tempDF.addColumn(labelColumnName, output.getDoubleColumnByName(labelColumnName));
        // add the scores column
        tempDF.addColumn(outputColumnName, scoresReversed);
        // add the window order column
        tempDF.addColumn(orderedWindow, output.getDoubleColumnByName(orderedWindow));
        output = tempDF.copy();
    }

    private void contaminatedDF() {
        DataFrame tempDF = new DataFrame();
        // Synchronize the scores and labels of the contaminated points
        double[] scores = output.getDoubleColumnByName(outputColumnName);
        List<Double> tempLB = new ArrayList<>();
        List<Double> tempSC = new ArrayList<>();
        for (int s = 0; s < scores.length; s++) {
            if (scores[s] != inlierScore) { // outlier scores
                tempSC.add(scores[s]);
                tempLB.add(output.getDoubleColumnByName(labelColumnName)[s]);
            }
        }
        // Synchronize the window order of the contaminated points
        List<Double> tempWO = new ArrayList<>();
        for (int s = 0; s < scores.length; s++) {
            if (scores[s] != inlierScore) {
                tempWO.add(output.getDoubleColumnByName(orderedWindow)[s]);
            }
        }
        // Synchronize the feature columns of the contaminated points
        for (String column : columns) {
            List<Double> tempCol = new ArrayList<>();
            for (int s = 0; s < scores.length; s++) {
                if (scores[s] != inlierScore) { // outliers
                    tempCol.add(output.getDoubleColumnByName(column)[s]);
                }
            }
            // add the features
            tempDF.addColumn(column, tempCol.stream().mapToDouble(Double::doubleValue).toArray());
        }
        // add the labels
        tempDF.addColumn(labelColumnName, tempLB.stream().mapToDouble(Double::doubleValue).toArray());
        // add the scores
        tempDF.addColumn(outputColumnName, tempSC.stream().mapToDouble(Double::doubleValue).toArray());
        // add the window order
        tempDF.addColumn(orderedWindow, tempWO.stream().mapToDouble(Double::doubleValue).toArray());
        // update the output, by keeping only the contaminated data points
        output = tempDF.copy();
    }

    // =============================================================================================================== //
    // - - - - - - - - - - - - - - - - - - - - SUB  METHODS OF THE HST ALGORITHM - - - - - - - - - - - - - - - - - - - //
    // =============================================================================================================== //

    private List<Integer> subSampling(int originalSize, int subSampleSize, boolean sampling) {
        // build the original size list of indexes.
        List<Integer> original = new ArrayList<>();
        for (int i = 0; i < originalSize; i++) {
            original.add(i);
        }
        if (sampling) {
            // shuffle the original list.
            Collections.shuffle(original);
            // take a sample of the original list.
            List<Integer> sample = new ArrayList<>();
            for (int i = 0; i < subSampleSize; i++) {
                sample.add(original.get(i));
            }
            // return the sample list.
            return sample;
        } else {
            return original;
        }
    }

    private List<double[]> subInstances(List<double[]> original, List<Integer> subIndexes) {
        List<double[]> sampleData = new ArrayList<>();
        for (int cur : subIndexes) {
            sampleData.add(original.get(cur));
        }
        return sampleData;
    }

    private double uniform(double min, double max) {
        return min + (max - min) * new Random().nextDouble();
    }

    private Map<String, double[]> workSpaceHST(List<double[]> window) {
        int numInst = window.size();
        int dimInst = window.get(0).length;
        double[] maxAtt = new double[dimInst];
        double[] minAtt = new double[dimInst];
        for (int dim = 0; dim < dimInst; dim++) {
            double highest = window.get(0)[dim];
            double lowest = window.get(0)[dim];
            // Calculate max/min column values:
            for (int row = 0; row < numInst; row++) {
                double featureValue = window.get(row)[dim];
                if (featureValue > highest) {
                    highest = featureValue;
                }
                if (featureValue < lowest) {
                    lowest = featureValue;
                }
            }
            // Generate a real number (S) randomly and uniformly from the range of the current dimension (q)
            double split = uniform(lowest, highest);
            // Calculate the work range of the current dimension (q)
            double workRange = 2 * Math.max(split, highest - split);
            // Calculate the workspace of the current dimension (q)
            maxAtt[dim] = split + workRange;
            minAtt[dim] = split - workRange;
        }
        Map<String, double[]> workspace = new HashMap<>();
        workspace.put(wMaxAtt, maxAtt);
        workspace.put(wMinAtt, minAtt);
        return workspace;
    }

    private double score(double mass, double depth) {
        if (mass < 2) {
            return depth;
        } else {
            return (depth + Math.log(mass));
        }
    }

    private Map<String, List<Integer>> treeTraverse(List<double[]> window, List<Integer> indexes, int splitAttribute, double splitPoint) {
        List<Integer> leftIndexSub = new ArrayList<>();
        List<Integer> rightIndexSub = new ArrayList<>();
        for (int cur : indexes) {
            if (window.get(cur)[splitAttribute] <= splitPoint) {
                leftIndexSub.add(cur);
            } else {
                rightIndexSub.add(cur);
            }
        }
        HashMap<String, List<Integer>> guidelines = new HashMap<>();
        guidelines.put(traverseLeft, leftIndexSub);
        guidelines.put(traverseRight, rightIndexSub);
        return guidelines;
    }

    private void readTreeFeatures(Node node, List<Integer> treeFeatures) {
        if (node.nodeStatus == 1) {
            treeFeatures.add(node.splitAttribute);
            readTreeFeatures(node.rightChild, treeFeatures);
            readTreeFeatures(node.leftChild, treeFeatures);
        }
    }

    private void logger_tree_features_csv() throws IOException {
        FileWriter csvWriter = new FileWriter(loggerWindowForestFeaturesPath);
        csvWriter.append(orderedWindow);
        csvWriter.append(",");
        csvWriter.append(treeTag);
        csvWriter.append(",");
        csvWriter.append(featuresTag);
        csvWriter.append("\n");
        Set<Integer> windowsID = pWindowForestFeatures.keySet();
        for (Integer wID : windowsID) {
            Collection<List<List<Integer>>> wIDCollection = pWindowForestFeatures.get(wID);
            for (List<List<Integer>> forestFeatures : wIDCollection) {
                for (int treeIndex = 0; treeIndex < forestFeatures.size(); treeIndex++) {
                    int treeID = treeIndex + 1;
                    String features = Joiner.on(';').join(forestFeatures.get(treeIndex));
                    csvWriter.append(String.valueOf(wID)).append(", ").append(String.valueOf(treeID)).append(", ").append(features);
                    csvWriter.append("\n");
                }
            }
        }
        csvWriter.flush();
        csvWriter.close();
    }

    private DataFrame removeOutliers(DataFrame df) {
        double[] labels = df.getDoubleColumnByName(labelColumnName);
        DataFrame df_new = new DataFrame();
        for (String column : columns) {
            List<Double> dimV = new ArrayList<>();
            for (int i = 0; i < labels.length; i++) {
                if (labels[i] < 1) {
                    dimV.add(df.getDoubleColumnByName(column)[i]);
                }
            }
            df_new.addColumn(column, dimV.stream().mapToDouble(Double::doubleValue).toArray());
        }
        List<Double> labelsP = new ArrayList<>();
        for (double label : labels) {
            if (label < 1) {
                labelsP.add(label);
            }
        }
        df_new.addColumn(labelColumnName, labelsP.stream().mapToDouble(Double::doubleValue).toArray());
        return df_new;
    }

    private void printTree(Node node) {
        node.print();
        if (node.nodeStatus == 1) {
            System.out.println("RIGHT");
            printTree(node.rightChild);
            System.out.println("LEFT");
            printTree(node.leftChild);
        }
    }


}