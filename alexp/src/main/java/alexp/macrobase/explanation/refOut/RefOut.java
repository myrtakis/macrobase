package alexp.macrobase.explanation.refOut;

import alexp.macrobase.explanation.Explanation;
import alexp.macrobase.explanation.utils.Subspace;
import alexp.macrobase.explanation.utils.RandomFactory;
import alexp.macrobase.pipeline.Pipelines;
import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.settings.ExplanationSettings;
import com.github.chen0040.data.frame.OutputDataColumn;
import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;

import javafx.util.Pair;


public class RefOut extends Explanation {

    private int d1;
    private int d2;
    private  int psize;
    private int opct;


    public RefOut(String[] columns, AlgorithmConfig classifierConf, ExplanationSettings explanationSettings) {
        super(columns, classifierConf, explanationSettings);
    }

    @java.lang.Override
    public <T> void addRelSubspaceColumnToDataframe(DataFrame data, T pointsSubspaces) {

    }

    private RandomFactory rnd = new RandomFactory((long)0);


    /**
     * The output DataFrame.
     */
    private DataFrame output;

    /*
        OVERRIDE FUNCTIONS
     */

    @java.lang.Override
    public void process(DataFrame input) throws Exception {
        output = input.copy();
        calculateSubspaces(input);
        //get all subspaces and
        // random subspace of dim d1

    }

    private void preProcessingOutlierScores(DataFrame input,int d1, int d2, int psize, int opct){

    }

    private <T> void refineSubspaces(){

    }

    private void calculateSubspaces(DataFrame input) throws Exception{
        //List of subspaces which are in P1
        HashSet<String> tmpP1 = new HashSet<>(psize);
        List<Subspace> P1 = new ArrayList<>();
        int counter = 0;
        int card = getDatasetDimensionality();
        System.out.println(card);
        while (counter < psize){
            Subspace newSubspace = new Subspace();
            while (newSubspace.getDimensionality() < d1){
                for (int featureId = new Random().nextInt(card); featureId >= 0; featureId--) {
                    System.out.println(featureId);
                    newSubspace.setFeature(featureId);
                    if(newSubspace.getDimensionality() == d1)
                        break;
                }
            }
            System.out.println("--------");
            if(!tmpP1.contains(newSubspace.getFeatures().toString())) {
                tmpP1.add(newSubspace.getFeatures().toString());
                System.out.println(tmpP1);
                counter++;
            }

        }

        System.out.println("The P1: " + tmpP1);

        for(String subspaceStr : tmpP1){
            P1.add(toSubspace(subspaceStr));
        }

        //Apply the score funnction on each subspace
        // Parcourir la liste P1 et appliquer le score function a chaque subspace et ensuite récxupérer le score

        List<List<Pair<Subspace, Double>>> pointsScoresInSubspaces = new ArrayList<>(input.getNumRows());
        Map <Integer,List<Subspace>> rankedSpaces = new HashMap<>(input.getNumRows());

        //Scoring each subspace or each point of each subspace?
        for (Subspace space : P1) {
            DataFrame tmpDataFrame = new DataFrame();
            String[] subColumns = new String[space.getDimensionality()];
            counter = 0;

            for (int featureId : space.getFeatures()) {
                tmpDataFrame.addColumn(columns[featureId], input.getDoubleColumn(featureId));
                subColumns[counter++] = columns[featureId];
            }
            Classifier classifier = Pipelines.getClassifier(classifierConf.getAlgorithmId(), classifierConf.getParameters(), subColumns);
            classifier.process(tmpDataFrame);
            DataFrame results = classifier.getResults();
            double[] scores = results.getDoubleColumnByName(outputColumnName);
            int j = 0;
            for(double score: scores){
                List<Subspace> subspaceList = rankedSpaces.getOrDefault(j, new ArrayList<>());
                subspaceList.add(new Subspace(space.getFeatures(), score));
                rankedSpaces.put(j, subspaceList);
                j++;
            }
           // updatePointsScoresInSubspace(space, results.getDoubleColumnByName(outputColumnName), pointsScoresInSubspaces);
        }
            //what's the point to index here  instead of d1 one should put something else

       }

        private Subspace toSubspace(String subspaceStr) {
            //String[] parts = subspaceStr.split("\\[, \\]");
            System.out.println(subspaceStr);
            String[] parts = subspaceStr.split("^\\[|, |\\]");
            Subspace subspace = new Subspace();
            for(String featureStr : parts){
                if (!featureStr.equals("")) {
                    System.out.println("FeatureStr:" + featureStr);
                    subspace.setFeature(Integer.parseInt(featureStr));
                }
            }
            return subspace;
        }

    private void updatePointsScoresInSubspace(Subspace subspace, double[] pointScores,
                                              List<List<Pair<Subspace, Double>>> pointsScoresInSubspaces){
        for(int i = 0; i < pointScores.length; i++){
            if(pointsScoresInSubspaces.size() <= i)
                pointsScoresInSubspaces.add(null);
            List<Pair<Subspace, Double>> subspacesScores = pointsScoresInSubspaces.get(i);
            Pair<Subspace, Double> pair = new Pair<>(subspace, pointScores[i]);
            if(subspacesScores == null)
                subspacesScores = new ArrayList<>();
            subspacesScores.add(pair);
            pointsScoresInSubspaces.set(i, subspacesScores);
        }
    }

    public void setD1(int d1) {
        this.d1 = d1;
    }

    public void setD2(int d2) {
        this.d2 = d2;
    }

    public void setPsize(int psize) {
        this.psize = psize;
    }

    public void setOpct(int opct) {
        this.opct = opct;
    }
    @java.lang.Override
    public DataFrame getResults() {
        return null;
    }
}
