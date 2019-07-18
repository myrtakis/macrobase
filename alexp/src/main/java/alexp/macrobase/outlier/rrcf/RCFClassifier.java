package alexp.macrobase.outlier.rrcf;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.outlier.Trainable;
import alexp.macrobase.outlier.Updatable;
import alexp.macrobase.utils.DataFrameUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Doubles;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.lang3.ArrayUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// last checked: 17/07/2019 - Report: All (debugging and correctness) tests passed!

public class RCFClassifier extends MultiMetricClassifier implements Trainable, Updatable {

    // Constant Parameters:
    private String minBox = "minValue";
    private String maxBox = "maxValue";
    private String wsValueRange = "ValueRange";
    private String wsMinAtt = "minAtt";
    private String wsMaxAtt = "maxAtt";
    private String labelColumnName = "is_anomaly"; // This parameter has to be inherited by the MultiMetricClassifier
    private String traverseLeft = "traverseLeft";
    private String traverseRight = "traverseRight";
    private String orderedWindow = "_WINDOW";
    private String treeTag = "_TREE";
    private String featuresTag = "_FEATURES";

    // Controlling variables:
    private boolean trainable = true;
    private boolean processable = false;
    private boolean updatable = false;
    private List<double[]> trainWindow = new ArrayList<>();
    private int trainSize = 0;
    private int processWindowCounter = 0;
    private Multimap<Integer, List<List<Integer>>> pWindowForestFeatures = ArrayListMultimap.create();

    // Hyper Parameters:
    private int numTree;            // Number of Trees
    private int numSub;             // Number of Leaves
    private int forgetThreshold;    // Threshold (bottom) to apply the forget mechanism
    private boolean shingle;        // Transform a window of data points into a shingle (one data point).
    private String datasetID;

    // Model and Output:
    private List<Node> forest = new ArrayList<>();
    private int[] forestMaxLeafAge; // The maximum leaf age per tree
    private int[] forestMinLeafAge; // The minimum leaf age per tree
    private DataFrame output;

    // Setters & Getters:
    public void setNumTree(int numTree) {
        this.numTree = numTree;
        this.forestMaxLeafAge = new int[numTree];
        this.forestMinLeafAge = new int[numTree];
    }

    public void setNumSub(int numSub) {
        this.numSub = numSub;
    }

    public void setForgetThreshold(int forgetThreshold) {
        this.forgetThreshold = forgetThreshold;
    }

    public void setShingle(boolean shingle) {
        this.shingle = shingle;
    }

    public void setDatasetID(String datasetID) {
        this.datasetID = datasetID;
    }



    // Constructor
    public RCFClassifier(String[] columns) {
        super(columns);
    }

    // A single Robust Random Cut Tree structure, consists of a Node, Branch and Leaf classes

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                  TRAINING PHASE
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public void train(DataFrame input) {
        if (trainable) {                                  // RCF model must be initialize only once
            List<double[]> inputWindow;
            if (shingle) {
                inputWindow = shingle((DataFrameUtils.toRowArray(removeOutliers(input), columns)));
            } else {
                inputWindow = (DataFrameUtils.toRowArray(removeOutliers(input), columns));
            }
            // Increment by the size of real input data (inliers + outliers)
            trainSize += (DataFrameUtils.toRowArray(input, columns)).size();
            // Add only the inliers into the train window
            trainWindow.addAll(inputWindow);
            if (trainSize >= numSub) { // The size of training window must be at least numTree * numSub
                buildClassifier(trainWindow);
                System.out.println("Training window size (inliers only): " + trainWindow.size() + " (inliers + outliers): " + trainSize);
                trainable = false;
                trainWindow.clear();
                //System.out.println("==========================================");
                //System.out.println("============== INITIAL TREE ==============");
                //System.out.println("==========================================");
                //printTree(forest.get(0));
                //System.out.println("==========================================");
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // 1. CONSTRUCT AN RRCF MODEL
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    private void buildClassifier(List<double[]> window) {
        int numInst = window.size(); // number of data points.
        for (int i = 0; i < numTree; i++) {
            List<Integer> indexSub;  // indexes of the selected (sub) samples.
            // Sampling with replacement: samples (data points).
            if (numSub < numInst && numSub > 0) {
                indexSub = subSampling(numInst, numSub, true);
            } else {
                indexSub = subSampling(numInst, numSub, false);
            }
            List<double[]> subWindow = synWindow(window, indexSub);
            Node root = treeBuilder(null, subWindow, 0, i);
            // Append the constructed tree to the model.
            forest.add(root);
        }
    }

    private Node treeBuilder(Node parent, List<double[]> window, int curtDepth, int treeIndex) {
        if (window.size() > 1 && !isRedundant(window)) {
            Branch newBranch = new Branch();
            newBranch.u = parent;
            Map<String, double[]> workspace = workspace(window);
            newBranch.q = randomChoice(workspace.get(wsValueRange));
            newBranch.p = uniform(workspace.get(wsMinAtt)[newBranch.q], workspace.get(wsMaxAtt)[newBranch.q]);
            newBranch.n = window.size();
            Map<String, double[]> box = new HashMap<>();
            box.put(minBox, workspace.get(wsMinAtt));
            box.put(maxBox, workspace.get(wsMaxAtt));
            newBranch.b = box;
            Map<String, List<double[]>> guide = treeTraverse(window, newBranch.q, newBranch.p);
            newBranch.r = treeBuilder(newBranch, guide.get(traverseRight), curtDepth + 1, treeIndex);
            newBranch.l = treeBuilder(newBranch, guide.get(traverseLeft), curtDepth + 1, treeIndex);
            return newBranch;
        } else {
            Leaf newLeaf = new Leaf();
            newLeaf.i = forestMaxLeafAge[treeIndex]++;
            newLeaf.d = curtDepth;
            newLeaf.u = parent;
            newLeaf.x = window.get(0);
            newLeaf.n = window.size(); // Duplicates are supported: n > 1 || n == 1.
            return newLeaf;
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                  UPDATE PHASE
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public void update(DataFrame input) {
        List<double[]> inputWindow;
        if (shingle) {
            inputWindow = shingle((DataFrameUtils.toRowArray(input, columns)));
        } else {
            inputWindow = (DataFrameUtils.toRowArray(input, columns));
        }
        if (!trainable) {
            if (updatable) {
                double[] anomalyScores = updateAndScore(inputWindow);
                outputBuilder(input, anomalyScores);
            } else {
                updatable = true;
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // 2. UPDATE AN RRCF MODEL
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    private double[] updateAndScore(List<double[]> window) {
        double[] avgCoDisp = new double[window.size()];
        for (int d = 0; d < window.size(); d++) {
            for (int t = 0; t < numTree; t++) {
                updateDecision(t, window.get(d));             // Update the tree structure
                double coDisp = coDispTree(t, window.get(d)); // Compute the collusive displacement of the tree
                avgCoDisp[d] += (coDisp / numTree);           // Compute the collusive displacement of the forest
            }
            //System.out.println("The Average Collusive Displace of [" + d + "] is = " + avgCoDisp[d]);
        }
        return avgCoDisp;
    }

    private void updateDecision(int treeIndex, double[] newDP) {
        // [UPDATE] REMOVE THE OLDEST LEAF (FIFO)
        if (forest.get(treeIndex).n > forgetThreshold && forest.get(treeIndex) instanceof Branch) {
            forgetPoint(treeIndex);
            //System.out.println("==========================================");
            //System.out.println("============== DELETED TREE ==============");
            //System.out.println("==========================================");
            //printTree(forest.get(treeIndex));
            //System.out.println("==========================================");
        }
        // [UPDATE] INSERT A NEW DATA POINT
        insertPoint(treeIndex, newDP);
        //System.out.println("==========================================");
        //System.out.println("============== INSERTED TREE ==============");
        //System.out.println("==========================================");
        //printTree(forest.get(treeIndex));
        //System.out.println("==========================================");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // 3.1 INSERT DATA POINT
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    private void insertPoint(int treeIndex, double[] newDP) {
        addPoint(newDP, forest.get(treeIndex), treeIndex);
        forest.set(treeIndex, getRoot(forest.get(treeIndex)));
        updateDepth(forest.get(treeIndex), 0);
        updateNOC(forest.get(treeIndex));
    }

    private void addPoint(double[] newDP, Node node, int treeIndex) {
        // BRANCH NODE
        if (node instanceof Branch) {
            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
            int splitFeature = ((Branch) node).q;
            double bBoxMinValue = ((Branch) node).b.get(minBox)[splitFeature];
            double bBoxMaxValue = ((Branch) node).b.get(maxBox)[splitFeature];
            double splitPoint = ((Branch) node).p;
            Node leftNode = ((Branch) node).l;
            Node rightNode = ((Branch) node).r;
            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
            // Inside bounding box
            if (isInsideBBox(newDP[splitFeature], bBoxMinValue, bBoxMaxValue)) {
                //System.out.println("INSIDE BOUNDING BOX");
                if (newDP[splitFeature] <= splitPoint) {
                    addPoint(newDP, leftNode, treeIndex);
                } else {
                    addPoint(newDP, rightNode, treeIndex);
                }
            }
            // Outside bounding box
            else {
                //System.out.println("OUTSIDE BOUNDING BOX");
                addSubTree(node, newDP, treeIndex);
            }
        }
        // LEAF NODE
        else if (node instanceof Leaf) {
            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
            double[] storedDataPoint = ((Leaf) node).x;
            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
            // The new data point does not already exists in tree
            if (!Arrays.equals(storedDataPoint, newDP)) {
                //System.out.println("NEW DATA POINT");
                addSubTree(node, newDP, treeIndex);
            }
            // The new data point already exists in the tree
            else {
                //System.out.println("ALREADY EXISTS");
                addDuplicatedLeaf((Leaf) node);
            }
        }
    }

    private void buildBranch(Branch newBranch, Node clonedNode, Leaf newLeaf) {
        // LEAF NODE:
        if (clonedNode instanceof Leaf) {
            if (clonedNode.u instanceof Branch) {
                double[] lowerBox = ((Branch) clonedNode.u).b.get(minBox);
                double[] upperBox = ((Branch) clonedNode.u).b.get(maxBox);
                newBranch.b = updateBBox(lowerBox, upperBox, newLeaf.x);
            } else {
                List<double[]> window = new ArrayList<>();
                window.add(newLeaf.x);
                window.add(((Leaf) clonedNode).x);
                Map<String, double[]> workspace = workspace(window);
                Map<String, double[]> box = new HashMap<>();
                box.put(minBox, workspace.get(wsMinAtt));
                box.put(maxBox, workspace.get(wsMaxAtt));
                newBranch.b = box;
            }
        }
        // BRANCH NODE:
        else if (clonedNode instanceof Branch) {
            double[] lowerBox = ((Branch) clonedNode).b.get(minBox);
            double[] upperBox = ((Branch) clonedNode).b.get(maxBox);
            newBranch.b = updateBBox(lowerBox, upperBox, newLeaf.x);
        }
        newBranch.u = clonedNode.u;
        newBranch.n = clonedNode.n + newLeaf.n;
        newBranch.q = randomChoice(normalizedVR(newBranch.b.get(minBox), newBranch.b.get(maxBox)));
        newBranch.p = uniform(newBranch.b.get(minBox)[newBranch.q], newBranch.b.get(maxBox)[newBranch.q]);
        setChildNodes(newBranch, newLeaf, clonedNode);
    }

    private void buildLeaf(Leaf newLeaf, Branch newBranch, double[] newDP, int treeIndex) {
        newLeaf.u = newBranch;
        newLeaf.i = forestMaxLeafAge[treeIndex]++;
        newLeaf.x = newDP;
        newLeaf.n = 1;
        newLeaf.d = 0; // it is lazy updated.
    }

    private void addDuplicatedLeaf(Leaf clonedNode) {
        clonedNode.n += 1; // Increment the number of duplicated data points
    }

    private void addSubTree(Node node, double[] newDP, int treeIndex) {
        Leaf newLeaf = new Leaf();                       // [1] Create the new Leaf
        Branch newBranch = new Branch();                 // [2] Create the new Branch
        connectAsChild(node, node.u, newBranch);         // [3] Connect the newBranch as left or right child, to the parent of the cloned node
        buildLeaf(newLeaf, newBranch, newDP, treeIndex); // [4] Build the attributes of the new Leaf (i, d, x, u, n)
        buildBranch(newBranch, node, newLeaf);           // [5] Build the attributes of the new Branch (q, p, l, r, b, u, n)
        connectParent(node, newBranch);                  // [6] Connect the newBranch as parent of the cloned node
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // 3.2 FORGET DATA POINT
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    private void forgetPoint(int treeIndex) {
        removePoint(forest.get(treeIndex), treeIndex);
        forestMinLeafAge[treeIndex]++;
        forest.set(treeIndex, getRoot(moveRootCursor(forest.get(treeIndex)))); // Find the new root (leaf or branch) of the decreased cloned tree
        updateDepth(forest.get(treeIndex), 0);
        updateNOC(forest.get(treeIndex));
    }

    private void removePoint(Node node, int treeIndex) {
        // BRANCH NODE:
        if (node instanceof Branch) {
            removePoint(((Branch) node).r, treeIndex);
            removePoint(((Branch) node).l, treeIndex);
        }
        // LEAF NODE:
        else if (node instanceof Leaf) {
            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
            Branch clonedParent = (Branch) node.u;
            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
            if (((Leaf) node).i == forestMinLeafAge[treeIndex]) {           // Find the oldest leaf node:
                Node siblingNode = siblingNode(clonedParent, node);
                connectAsChild(clonedParent, clonedParent.u, siblingNode);  // [1] Set the sibling node as right or left child of the grand parent of the cloned node
                connectParent(siblingNode, clonedParent.u);                 // [2] Set the grand parent of the cloned node as parent to the sibling node
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                PREDICTION PHASE
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public void process(DataFrame input) {
        if (!trainable) {
            if (processable) {

                processWindowCounter++;

                if (shingle) {
                    System.out.println("Processing (shingle) window size: " + shingle((DataFrameUtils.toRowArray(input, columns))).size());
                } else {
                    System.out.println("Processing window size: " + (DataFrameUtils.toRowArray(input, columns)).size());
                }

                // CREATE A MAP OF THE CURRENT WINDOW -> TREES -> FEATURES
                List<List<Integer>> forestFeatures = new ArrayList<>();
                for (Node node : forest) {
                    List<Integer> treeFeatures = new ArrayList<>();
                    readTreeFeatures(node, treeFeatures);
                    forestFeatures.add(treeFeatures);
                }
                pWindowForestFeatures.put(processWindowCounter, forestFeatures);


            } else {
                processable = true;
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // 3. ANOMALY DETECTION
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    private double coDispTree(int treeIndex, double[] newDP) {
        List<Double> displacement = new ArrayList<>();
        disp(forest.get(treeIndex), newDP, displacement); // Compute the Displacement
        return coDisp(displacement);                      // Compute the Collusive Displacement
    }

    private void disp(Node node, double[] newDP, List<Double> displacement) {
        // BRANCH NODE
        if (node instanceof Branch) {
            int rLeaves = ((Branch) node).r.n;
            int lLeaves = ((Branch) node).l.n;
            if (newDP[((Branch) node).q] <= ((Branch) node).p) {
                displacement.add((rLeaves / (double) lLeaves));
                disp(((Branch) node).l, newDP, displacement);
            } else {
                displacement.add((lLeaves / (double) rLeaves));
                disp(((Branch) node).r, newDP, displacement);
            }
        }
        // LEAF NODE
        else {
            displacement.add(0.0);
        }
    }

    private double coDisp(List<Double> displacement) {
        return Collections.max(displacement);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

    /*
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     *                FINAL RESULT OUTPUT
     * = = = = = = = = = = = = = = = = = = = = = = = = =
     * */

    @Override
    public DataFrame getResults() {
        System.out.println("Resulting the final output..");
        /*
        System.out.println("======================== DATA POINTS ==========================");
        double[] dps = output.getDoubleColumnByName(labelColumnName);
        System.out.println("Total Data Points = " + dps.length);
        System.out.println("---------------------------------------------------------------");
        for (double g : dps) {
            System.out.println(g);
        }
        System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
        double[] datas = output.getDoubleColumnByName("d1");
        for (double dddd : datas) {
            System.out.println(dddd);
        }
        System.out.println("===============================================================");
        */

        /*
        System.out.println("======================= ANOMALY SCORES ========================");
        double[] results = output.getDoubleColumnByName(outputColumnName);
        System.out.println("Total Scores = " + results.length);
        System.out.println("---------------------------------------------------------------");
        for (double r : results) {
            System.out.println(r);
        }
        System.out.println("===============================================================");
        */
        // EXPORT THE WINDOW/TREES/FEATURES INTO A CSV
        try {
            String loggerWindowForestFeaturesPath = "./alexp/output/rrcf#" + beautifyDatasetID() + "#windowForestFeatures.csv";
            logger_tree_features_csv(loggerWindowForestFeaturesPath);
            System.out.println("[Logger] The Window/Trees/Features information has been exported at "+loggerWindowForestFeaturesPath+"...");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return output;
    }

    // =============================================================================================================== //
    // - - - - - - - - - - - - - - - - - - - - SUB-METHODS OF THE RRCF ALGORITHM - - - - - - - - - - - - - - - - - - - //
    // =============================================================================================================== //

    private String beautifyDatasetID() {
        List<String> collapsedID = Arrays.asList(datasetID.split("/"));
        return collapsedID.get(collapsedID.size() - 1).replace(".csv", "");
    }


    private Map<String, List<double[]>> treeTraverse(List<double[]> window, int splitFeature, double splitValue) {
        List<double[]> windowLeft = new ArrayList<>();
        List<double[]> windowRight = new ArrayList<>();
        for (double[] dp : window) {
            if (dp[splitFeature] <= splitValue) {
                windowLeft.add(dp);
            } else {
                windowRight.add(dp);
            }
        }
        HashMap<String, List<double[]>> guidelines = new HashMap<>();
        guidelines.put(traverseLeft, windowLeft);
        guidelines.put(traverseRight, windowRight);
        return guidelines;
    }

    private int updateNOC(Node clonedNode) {
        if (clonedNode instanceof Branch) {
            clonedNode.n = updateNOC(((Branch) clonedNode).r) + updateNOC(((Branch) clonedNode).l);
        }
        return clonedNode.n;
    }

    private boolean isRedundant(List<double[]> window) {
        return window.stream().allMatch(e -> (Arrays.toString(e)).equals(Arrays.toString(window.get(0))));
    }

    private boolean isInsideBBox(double value, double min, double max) {
        if (value >= min && value <= max) {
            return true;
        } else {
            return false;
        }
    }

    private Map<String, double[]> updateBBox(double[] minValues, double[] maxValues, double[] newDP) {
        double[] newBMin = new double[minValues.length];
        double[] newBMax = new double[maxValues.length];
        for (int i = 0; i < newDP.length; i++) {
            newBMin[i] = minValues[i];
            newBMax[i] = maxValues[i];
            if (newDP[i] < newBMin[i]) {
                newBMin[i] = newDP[i];
            } else if (newDP[i] > newBMax[i]) {
                newBMax[i] = newDP[i];
            }
        }
        Map<String, double[]> updatedBox = new HashMap<>();
        updatedBox.put(minBox, newBMin);
        updatedBox.put(maxBox, newBMax);
        return updatedBox;
    }

    private void updateDepth(Node clonedNode, int curtDepth) {
        if (clonedNode instanceof Leaf) {
            ((Leaf) clonedNode).d = curtDepth;
        } else if (clonedNode instanceof Branch) {
            updateDepth(((Branch) clonedNode).r, curtDepth + 1);
            updateDepth(((Branch) clonedNode).l, curtDepth + 1);
        }
    }

    private Node siblingNode(Branch parent, Node clonedNode) {
        if (parent.r == clonedNode) {
            return parent.l;
        } else {
            return parent.r;
        }
    }

    private void connectAsChild(Node clonedNode, Node clonedParent, Node newNode) {
        if (clonedParent instanceof Branch) {
            if (((Branch) clonedParent).l == clonedNode) {
                ((Branch) clonedParent).l = newNode;
            } else {
                ((Branch) clonedParent).r = newNode;
            }
        }
    }

    private void connectParent(Node child, Node parent) {
        child.u = parent;
    }

    private void setChildNodes(Branch newBranch, Leaf newLeaf, Node clonedNode) {
        if (clonedNode instanceof Leaf) {
            if (!isInsideBBox(newBranch.p, newLeaf.x[newBranch.q], ((Leaf) clonedNode).x[newBranch.q])) {
                newBranch.p = uniform(newLeaf.x[newBranch.q], ((Leaf) clonedNode).x[newBranch.q]);
            }
        }
        if (newLeaf.x[newBranch.q] <= newBranch.p) {
            newBranch.l = newLeaf;
            newBranch.r = clonedNode;
        } else {
            newBranch.r = newLeaf;
            newBranch.l = clonedNode;
        }
    }

    private Node cloneRCT(Node node) {
        return node.copy(null);
    }

    private int randomChoice(double[] featureRanges) {
        double[] featuresWeights = weightFeatures(featureRanges);
        // Compute the total weight of all items together
        double totalWeight = 0.0d;
        for (double weight : featuresWeights) {
            totalWeight += weight;
        }
        // Now choose a random item
        int randomIndex = -1;
        double random = Math.random() * totalWeight;
        for (int i = 0; i < featuresWeights.length; ++i) {

            random -= featuresWeights[i];
            if (random <= 0.0d) {
                randomIndex = i;
                break;
            }

        }
        return randomIndex;
    }

    private double[] weightFeatures(double[] featureRanges) {
        double sum = Doubles.asList(featureRanges).stream().mapToDouble(x -> x).sum();
        for (int i = 0; i < featureRanges.length; i++) {
            featureRanges[i] = featureRanges[i] / sum;
        }
        return featureRanges;
    }

    private List<double[]> synWindow(List<double[]> window, List<Integer> indexSub) {
        List<double[]> selectionWindow = new ArrayList<>();
        for (int index : indexSub) {
            selectionWindow.add(window.get(index));
        }
        return selectionWindow;
    }

    private double[] normalizedVR(double[] min, double[] max) {
        // calculate the value range of each dimension
        double[] vrAtt = new double[min.length];
        for (int col = 0; col < vrAtt.length; col++) {
            vrAtt[col] = max[col] - min[col];
        }
        // normalize the value range of each dimension as ( Value Range / sum(Value Ranges) )
        double sumVR = Arrays.stream(vrAtt).sum();
        if (sumVR > 0) {
            for (int col = 0; col < vrAtt.length; col++) {
                vrAtt[col] /= sumVR;
            }
        }
        return vrAtt;
    }

    private Map<String, double[]> workspace(List<double[]> window) {
        int numInst = window.size();
        int dimInst = window.get(0).length;
        double[] maxAtt = new double[dimInst];
        double[] minAtt = new double[dimInst];
        for (int col = 0; col < dimInst; col++) {
            double highest = window.get(0)[col];
            double lowest = window.get(0)[col];
            for (int row = 0; row < numInst; row++) {
                double featureValue = window.get(row)[col];
                if (featureValue > highest) {
                    highest = featureValue;
                }
                if (featureValue < lowest) {
                    lowest = featureValue;
                }
            }
            // Min and Max values per dimension
            maxAtt[col] = highest;
            minAtt[col] = lowest;
        }
        Map<String, double[]> workspace = new HashMap<>();
        workspace.put(wsMaxAtt, maxAtt);
        workspace.put(wsMinAtt, minAtt);
        workspace.put(wsValueRange, normalizedVR(minAtt, maxAtt));
        return workspace;
    }

    private double uniform(double min, double max) {
        double u = max;
        if (min != max) {
            while (u == max) {
                u = (min + (max - min) * new Random().nextDouble());
            }
        }
        return u; // valid range [min, max).
    }

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

    private void outputBuilder(DataFrame windowDF, double[] windowScores) {
        // IF Shingle is true, then the windowDF will be synchronized to the window Scores
        List<double[]> windowData = DataFrameUtils.toRowArray(windowDF, columns);
        double[] windowLabels = windowDF.getDoubleColumnByName(labelColumnName);
        List<double[]> synData = new ArrayList<>();
        double[] synLabels = new double[windowScores.length];
        for (int i = 0; i < windowScores.length; i++) {
            synData.add(windowData.get(i));
            synLabels[i] = windowLabels[i];
        }
        List<double[]> dimensions = new ArrayList<>();
        for (int d = 0; d < synData.get(0).length; d++) {
            double[] dimValues = new double[synData.size()];
            for (int i = 0; i < synData.size(); i++) {
                dimValues[i] = synData.get(i)[d];
            }
            dimensions.add(dimValues);
        }
        // Find the window order for each data point
        double[] windowOrder = new double[windowScores.length];
        for (int i = 0; i < windowOrder.length; i++) {
            windowOrder[i] = processWindowCounter;
        }
        // Build the new partial output
        DataFrame tempDF = new DataFrame();
        if (output != null) {
            // add the feature columns
            for (int c = 0; c < columns.length; c++) {
                tempDF.addColumn(columns[c], ArrayUtils.addAll(output.getDoubleColumnByName(columns[c]), dimensions.get(c)));
            }
            // add the label (ground truth) column
            tempDF.addColumn(labelColumnName, ArrayUtils.addAll(output.getDoubleColumnByName(labelColumnName), synLabels));
            // add the scores column
            tempDF.addColumn(outputColumnName, ArrayUtils.addAll(output.getDoubleColumnByName(outputColumnName), windowScores));
            // add the window order column
            tempDF.addColumn(orderedWindow, ArrayUtils.addAll(output.getDoubleColumnByName(orderedWindow), windowOrder));
            // update the new output
            output = tempDF.copy();
        } else {
            // add the feature columns
            for (int c = 0; c < columns.length; c++) {
                tempDF.addColumn(columns[c], dimensions.get(c));
            }
            // add the label (ground truth) column
            tempDF.addColumn(labelColumnName, synLabels);
            // add the scores column
            tempDF.addColumn(outputColumnName, windowScores);
            // add the window order column
            tempDF.addColumn(orderedWindow, windowOrder);
            // update the new output
            output = tempDF.copy();
        }
    }

    private Node getRoot(Node node) {
        while (node.u != null) {
            node = node.u;
        }
        return node;
    }

    private Node moveRootCursor(Node node) {
        if (node instanceof Branch) {
            return ((Branch) node).r.u == null ? ((Branch) node).r : ((Branch) node).l;
        } else {
            return node;
        }
    }

    private List<double[]> shingle(List<double[]> window) {
        int windowSize = window.size();
        int dimensions = window.get(0).length;
        int shingleSize = windowSize * dimensions;
        double[] shingleWindow = new double[shingleSize];
        int counter = 0;
        for (double[] point : window) {
            for (double dimValue : point) {
                shingleWindow[counter++] = dimValue;
            }
        }
        List<double[]> res = new ArrayList<>();
        res.add(shingleWindow);
        return res;
    }

    private List<double[]> normWindow(List<double[]> window) {
        int winSize = window.size();
        if (winSize > 1) {
            // 1. Find minimum/maximum value per dimension
            double[] dimsMax = new double[window.get(0).length];
            double[] dimsMin = new double[window.get(0).length];
            for (double[] dp : window) { // for each point
                for (int d = 0; d < dp.length; d++) { // for each dimension
                    if (dimsMax[d] < dp[d]) {
                        dimsMax[d] = dp[d];
                    }
                    if (dimsMin[d] > dp[d]) {
                        dimsMin[d] = dp[d];
                    }
                }
            }
            // 2. Normalize the window using the max value per dimension
            List<double[]> normalizedWindow = new ArrayList<>();
            for (double[] dp : window) {
                double[] normdp = new double[dp.length];
                for (int d = 0; d < dp.length; d++) {
                    normdp[d] = ((dp[d] - dimsMin[d]) / (dimsMax[d] - dimsMin[d]));
                }
                normalizedWindow.add(normdp);
            }
            return normalizedWindow;
        } else {
            return window;
        }
    }

    public DataFrame removeOutliers(DataFrame df) {
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

    private void readTreeFeatures(Node node, List<Integer> treeFeatures) {
        if (node instanceof Branch) {
            treeFeatures.add(((Branch) node).q);
            readTreeFeatures(((Branch) node).r, treeFeatures);
            readTreeFeatures(((Branch) node).l, treeFeatures);
        }
    }

    private void logger_tree_features_csv(String loggerWindowForestFeaturesPath) throws IOException {
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

    private void printTree(Node node) {
        node.print();
        if (node instanceof Branch) {
            System.out.println("RIGHT");
            printTree(((Branch) node).r);
            System.out.println("LEFT");
            printTree(((Branch) node).l);
        }
    }

}