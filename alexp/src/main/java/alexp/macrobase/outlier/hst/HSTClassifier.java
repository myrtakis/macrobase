package alexp.macrobase.outlier.hst;

import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.outlier.ParametersAutoTuner;
import alexp.macrobase.outlier.Trainable;
import alexp.macrobase.utils.DataFrameUtils;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;
import java.util.stream.Collectors;

public class HSTClassifier extends MultiMetricClassifier implements ParametersAutoTuner, Trainable {


    // Hyper parameters:
    private int numTree; // Number of Half Space Trees
    private int numSub; // Sub sample size
    private int numDim; // Sub dimensional size
    private int sizeLimit; // Leaf Size Limitation
    private int depthLimit; // Maximum depth limitation

    // Forest:
    private List<HST> forest = new ArrayList<>();

    // Output scored outliers:
    private DataFrame output;

    public void setNumTree(int numTree) {
        this.numTree = numTree;
    }

    public void setNumSub(int numSub) {
        this.numSub = numSub;
    }

    public void setNumDim(int numDim) {
        this.numDim = numDim;
    }

    public void setSizeLimit(int sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public void setDepthLimit(int depthLimit) {
        this.depthLimit = depthLimit;
    }

    public HSTClassifier(String[] columns) {
        super(columns);
    }

    // A single Half Space Tree (HST) structure
    public class HST {
        int nodeStatus;
        int splitAttribute;
        double splitPoint;
        HST leftChild;
        HST rightChild;
        double size;
        int depth;
    }

    /**
     * Implemented Functions from Parameter Tuning and Trainable
     */

    @Override
    public Map<String, Object> tuneParameters(DataFrame trainSet) {

        System.out.println("TUNE PARAMETERS");


        return null;
    }




    @Override
    public void process(DataFrame input) throws Exception {

        // Estimated mass per tree/points
        List<double[]> estimatedMass = estimateMass(DataFrameUtils.toRowArray(input, columns));

        // Normalized mass per point.
        double[] outlierScores = predictMass(estimatedMass);


        System.out.println("OUTLIER SCORES: ");

        for(int i = 0; i< outlierScores.length; i++){

            System.out.println("Point ID ["+i+"] Outlier Score: "+ outlierScores[i]);
        }

        // TODO: CHECK WHAT IS HAPPENING WITH THE MASS PROFILES. FOR SOME REASON IT RESULTS INFINITY AND NAN!
        System.out.println("--------------------------------------------------------------");

        System.out.println("PROCESS");
    }

    private List<double[]> estimateMass(List<double[]> window) {
        int numInst = window.size(); // number of data points in the current window

        int[] selectedPoints = new int[numInst];
        for (int i = 0; i < numInst; i++) {
            selectedPoints[i] = i + 1;
        }

        List<double[]> massForest = new ArrayList<>();
        for (int i = 0; i < numTree; i++) {
            double[] massTree = treeMass(window, selectedPoints, forest.get(i), new double[numInst]);
            massForest.add(massTree);
        }

        return massForest;
    }

    private double[] predictMass(List<double[]> mass) {
        int massTrees = mass.size();
        int massInstances = mass.get(0).length;
        // Calculate the (not normalized) outlier scores:
        double[] scores = new double[massInstances];
        for (int i = 0; i < massInstances; i++) { // for every instance repeat:
            double[] massDP = new double[massTrees];
            for (int j = 0; j < massTrees; j++) { // for every tree repeat:
                massDP[j] = mass.get(j)[i];
            }
            scores[i] = calculateMean(massDP);
        }
        return normalizeScores(scores);
    }

    private double calculateMean(double[] vector) {
        double sum = 0;
        for (int i = 0; i < vector.length; i++) {
            sum += vector[i];
        }
        return sum / vector.length;
    }

    private double[] normalizeScores(double[] scores) {
        // A. calculate the maximum outlier score of this window:
        boolean init = true;
        double maxScore = Double.MIN_VALUE;
        for (int i = 1; i < scores.length; i++) {
            if (!init) {
                if (scores[i] > maxScore) {
                    maxScore = scores[i];
                }
            } else {
                maxScore = scores[i];
                init = false;
            }
        }
        // B. normalize scores:
        for (int i = 0; i < scores.length; i++) {
            scores[i] = scores[i] / maxScore;
        }
        return scores;
    }

    private double[] treeMass(List<double[]> window, int[] indexesDP, HST selectedTree, double[] massTree) {
        // leaf node:
        if (selectedTree.nodeStatus == 0) {
            if (selectedTree.size < 2) {
                for (int i = 0; i < indexesDP.length; i++) {
                    massTree[i] = selectedTree.depth;
                }
            } else {
                for (int i = 0; i < indexesDP.length; i++) {
                    massTree[i] = selectedTree.depth + Math.log(selectedTree.size);
                }
            }
        }
        // internal node:
        else {
            List<Integer> indexesDPLeft = new ArrayList<>();
            for (int i = 0; i < indexesDP.length; i++) {
                double dpFValue = window.get(i)[selectedTree.splitAttribute];
                if (dpFValue < selectedTree.splitPoint) {
                    indexesDPLeft.add(indexesDP[i]);
                }
            }
            List<Integer> indexesDPRight = Arrays.stream(indexesDP).boxed().collect(Collectors.toList());
            indexesDPRight.removeAll(indexesDPLeft);
            if (!indexesDPLeft.isEmpty()) {
                treeMass(window, indexesDPLeft.stream().mapToInt(i -> i).toArray(), selectedTree.leftChild, massTree);
            }
            if (!indexesDPRight.isEmpty()) {
                treeMass(window, indexesDPRight.stream().mapToInt(i -> i).toArray(), selectedTree.rightChild, massTree);
            }
        }
        return massTree;
    }



















    @Override
    public DataFrame getResults() {


        System.out.println("GET RESULTS");


        return output;
    }


    @Override
    public void train(DataFrame input) throws Exception {
        buildClassifier(DataFrameUtils.toRowArray(input, columns));
    }

    private void buildClassifier(List<double[]> window) throws Exception {

        int numInst = window.size();        // number of data points.
        int dimInst = window.get(0).length; // number of dimensions.

        Map<String, Integer> paras = new HashMap<>(); // requested model parameters.
        paras.put("sizeLimit", sizeLimit);            // lowest size limit of leaf nodes.
        paras.put("depthLimit", depthLimit);          // maximum length limit of a tree.

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

            // Construct a single HST.
            HST constructedHST = treeBuilder(new HST(), window, workspace.get("maxAtt"), workspace.get("minAtt"),
                    indexSub, 0, indexDim, paras);

            // Append the constructed tree to the model.
            forest.add(constructedHST);
        }

        System.out.println("TRAIN");
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

    private Map<String, double[]> workSpaceHST(List<double[]> window) {

        int numInst = window.size();
        int dimInst = window.get(0).length;

        double[] maxAtt = new double[dimInst];
        double[] minAtt = new double[dimInst];

        for (int col = 0; col < dimInst; col++) {
            double highest = Double.MIN_VALUE;
            double lowest = Double.MAX_VALUE;

            // Calculate max/min column values:
            for (int row = 0; row < numInst; row++) {
                if (window.get(row)[col] > highest) {
                    highest = window.get(row)[col];
                }
                if (window.get(row)[col] < lowest) {
                    lowest = window.get(row)[col];
                }
            }

            // calculate the random split of the current column
            double splitA = highest + (highest - lowest) * (new Random().nextInt(1));

            // calculate the range of the current column
            double rangeA = 2 * Math.max((splitA - lowest), (highest - splitA));

            // calculate the workspace of all columns
            maxAtt[col] = splitA + rangeA;
            minAtt[col] = splitA - rangeA;

        }

        Map<String, double[]> workspace = new HashMap<>();
        workspace.put("maxAtt", maxAtt);
        workspace.put("minAtt", minAtt);
        return workspace;
    }

    private HST treeBuilder(HST tree, List<double[]> window, double[] maxAtt, double[] minAtt, List<Integer> indexSub, int curtDepth, List<Integer> indexDim, Map<String, Integer> paras) {
        tree.depth = curtDepth;
        if (curtDepth >= paras.get("depthLimit") || indexSub.size() <= paras.get("sizeLimit")) {
            tree.nodeStatus = 0; // leaf node.
            tree.splitAttribute = Integer.MIN_VALUE;
            tree.splitPoint = Integer.MIN_VALUE;
            tree.leftChild = null;
            tree.rightChild = null;
            tree.size = indexSub.size();
        } else {
            tree.nodeStatus = 1; // internal node.
            // Split Attribute (Feature): randomly selected.
            tree.splitAttribute = indexDim.get(new Random().nextInt(((indexDim.size() - 1)) + 1));
            // Split Point (Value): middle value of its work space.
            tree.splitPoint = (maxAtt[tree.splitAttribute] + minAtt[tree.splitAttribute]) / 2;
            // Indexes of samples that belong to the left side of the current tree
            List<Integer> leftIndexSub = new ArrayList<>();
            for (int i = 0; i < indexSub.size(); i++) {
                if (window.get(i)[tree.splitAttribute] < tree.splitPoint) {
                    leftIndexSub.add(i);
                }
            }
            // Indexes of samples that belong to the right side of the current tree
            List<Integer> rightIndexSub = new ArrayList<>(indexSub);
            rightIndexSub.removeAll(leftIndexSub);
            // Build: Left Tree
            double temp = maxAtt[tree.splitAttribute];
            maxAtt[tree.splitAttribute] = tree.splitPoint;
            tree.leftChild = treeBuilder(new HST(), window, maxAtt, minAtt, leftIndexSub, curtDepth + 1, indexDim, paras);
            // Build: Right Tree
            maxAtt[tree.splitAttribute] = temp;
            minAtt[tree.splitAttribute] = tree.splitPoint;
            tree.rightChild = treeBuilder(new HST(), window, maxAtt, minAtt, rightIndexSub, curtDepth + 1, indexDim, paras);
            tree.size = Integer.MIN_VALUE;
        }
        return tree;
    }

    private List<double[]> subInstances(List<double[]> original, List<Integer> subIndexes) {
        List<double[]> sampleData = new ArrayList<>();
        for (int i = 0; i < subIndexes.size(); i++) {
            int cursor = subIndexes.get(i);
            sampleData.add(original.get(cursor));
        }
        return sampleData;
    }


}
