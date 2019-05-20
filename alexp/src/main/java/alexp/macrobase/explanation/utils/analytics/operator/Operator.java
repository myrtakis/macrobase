package alexp.macrobase.explanation.utils.analytics.operator;

import alexp.macrobase.explanation.utils.analytics.config.AnalyticsConfig;
import alexp.macrobase.ingest.Uri;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;
import javafx.util.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Operator {

    AnalyticsConfig conf;
    private HashMap<String, DataFrame> explainersPointsAnalytics = new HashMap<>();
    private HashMap<String, DataFrame> explainerSubspaceAnalytics = new HashMap<>();

    // Point Analytics Column Names
    private final String pointIDsCol = "PointId";
    private final String avgScoreCol = "AvgScore";
    private final String maximumSubspaceScoreCol = "MaximumSubspaceScore";
    private final String minimumSubspaceScoreCol = "MinimumSubspaceScore";

    // Subspace Analytics Column Names
    private final String subspaceCol = "Subspace";
    private final String frequencyCol = "Frequency";
    private final String avgPointsScoreCol = "AvgPointsScore";

    // Folder Name
    private final String outputFolderName = "analytics";

    // File Names
    private final String pointAnalyticsFileName = "point_analytics";
    private final String subspacesAnalyticsFileName = "subspace_analytics";

    private static DecimalFormat df2 = new DecimalFormat("#.##");

    public Operator(AnalyticsConfig conf) {
        this.conf = conf;
    }

    public void operate(boolean compare) throws Exception {
        for(Pair<String, String> p : conf.getExplainers()){
            executeAnalyticActions(p.getKey(), getResultsOutputDataFrame(p.getValue()));
            writeToDisk(explainersPointsAnalytics.get(p.getKey()), p.getValue(), pointAnalyticsFileName);
            writeToDisk(explainerSubspaceAnalytics.get(p.getKey()), p.getValue(), subspacesAnalyticsFileName);
        }
    }

    private DataFrame getResultsOutputDataFrame(String inputFilePath) throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<String, Schema.ColType>(){
            {
                put(conf.getOutputColumn(), Schema.ColType.DOUBLE);
                put(conf.getRelSubspaceColumn(), Schema.ColType.STRING);
            }
        };
        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());
        DataFrame results = PipelineUtils.loadDataFrame(inputFilePath, colTypes, requiredColumns);

        DataFrame data = new DataFrame();
        List<Integer> pointsOfInterest = conf.getSettingsConfig().getExplanationSettings().getDictatedOutliers();
        double[] poiIDs = new double[pointsOfInterest.size()];
        double[] poiScores = new double[pointsOfInterest.size()];
        String[] poiRelSubspaces = new String[pointsOfInterest.size()];

        int counter = 0;
        for(int pointId : pointsOfInterest){
            poiIDs[counter] = pointId;
            poiScores[counter] = get2DigitPrecision(results.getRow(pointId).getAs(conf.getOutputColumn()));
            poiRelSubspaces[counter] = results.getRow(pointId).getAs(conf.getRelSubspaceColumn());
            counter++;
        }
        data.addColumn(pointIDsCol, poiIDs);
        data.addColumn(avgScoreCol, poiScores);
        data.addColumn(conf.getRelSubspaceColumn(), poiRelSubspaces);
        return data;
    }

    private void executeAnalyticActions(String explainerId, DataFrame resultsDataFrame) {
        List<List<Subspace>> pointsRelSubspaces = parseRelSubspaceList(resultsDataFrame);

        updatePointsAnalytics(explainerId, resultsDataFrame, pointsRelSubspaces);

        updateSubspacesAnalytics(explainerId, pointsRelSubspaces);
    }

    private void updatePointsAnalytics(String explainerId, DataFrame resultsDataFrame,
                                       List<List<Subspace>> pointsRelSubspaces) {
        DataFrame explainerAnalyticsOutput = new DataFrame();
        explainerAnalyticsOutput.addColumn(pointIDsCol, resultsDataFrame.getDoubleColumnByName(pointIDsCol));
        explainerAnalyticsOutput.addColumn(avgScoreCol, resultsDataFrame.getDoubleColumnByName(avgScoreCol));
        explainersPointsAnalytics.put(explainerId, explainerAnalyticsOutput);
        addMinMaxSubspace(explainerId, pointsRelSubspaces);
    }

    private void updateSubspacesAnalytics(String explainerId, List<List<Subspace>> pointsRelSubspaces) {
        HashMap<String, Pair<Integer, Double>> subspacesAnalytics = new HashMap<>();
        DataFrame explainerAnalyticsOutput = new DataFrame();
        for(List<Subspace> pointSubspaces : pointsRelSubspaces){
            subspacesFrequency(pointSubspaces, subspacesAnalytics);
        }
        String[] subspaces = new String[subspacesAnalytics.size()];
        double[] frequency = new double[subspacesAnalytics.size()];
        double[] avgScore = new double[subspacesAnalytics.size()];

        int counter = 0;
        for(String subspaceStr : subspacesAnalytics.keySet()){
            Pair<Integer, Double> pair = subspacesAnalytics.get(subspaceStr);
            subspaces[counter] = subspaceStr;
            frequency[counter] = pair.getKey();
            avgScore[counter] = get2DigitPrecision(pair.getValue() / pair.getKey()); // score / frequency
            counter++;
        }
        System.out.println(Lists.newArrayList(subspaces));
        explainerAnalyticsOutput.addColumn(subspaceCol, subspaces);
        explainerAnalyticsOutput.addColumn(frequencyCol, frequency);
        explainerAnalyticsOutput.addColumn(avgScoreCol, avgScore);

        explainerSubspaceAnalytics.put(explainerId, explainerAnalyticsOutput);
    }

    private void subspacesFrequency(List<Subspace> pointSubspaces, HashMap<String, Pair<Integer, Double>> subspacesAnalytics) {
        for(Subspace subspace : pointSubspaces) {
            Pair<Integer, Double> pair = subspacesAnalytics.getOrDefault(subspace.getFeatures().toString(), new Pair<>(0, 0.0));
            pair = new Pair<>(pair.getKey() + 1, pair.getValue() + subspace.getScore());
            subspacesAnalytics.put(subspace.getFeatures().toString(), pair);
        }
    }

    private void addMinMaxSubspace(String explainerId, List<List<Subspace>> pointsRelSubspaces) {
        DataFrame dataFrame = explainersPointsAnalytics.get(explainerId);
        String[] minSubspaces = new String[pointsRelSubspaces.size()];
        String[] maxSubspaces = new String[pointsRelSubspaces.size()];
        int counter = 0;
        for(List<Subspace> pointSubspaces : pointsRelSubspaces){
            pointSubspaces.sort(Comparator.comparing(Subspace::getScore));
            Subspace minumumSubspace = pointSubspaces.get(0);
            Subspace maximumSubspace = pointSubspaces.get(pointSubspaces.size() - 1);
            minSubspaces[counter] = minumumSubspace.toString();
            maxSubspaces[counter] = maximumSubspace.toString();
            counter++;
        }
        dataFrame.addColumn(maximumSubspaceScoreCol, maxSubspaces);
        dataFrame.addColumn(minimumSubspaceScoreCol, minSubspaces);
        explainersPointsAnalytics.put(explainerId, dataFrame);
    }

    private List<List<Subspace>> parseRelSubspaceList(DataFrame resultsDataFrame) {
        List<List<Subspace>> pointsRelSubspaces = new ArrayList<>();
        for(int i = 0; i < resultsDataFrame.getNumRows(); i++){
            String relSubspacesString = resultsDataFrame.getRow(i).getAs(conf.getRelSubspaceColumn());
            List<Subspace> subspacesList = new ArrayList<>();
            for(String featureScorePair : relSubspacesString.split(";")) {
                Subspace subspace = new Subspace();
                String[] features = featureScorePair.substring(featureScorePair.indexOf('[')+1, featureScorePair.indexOf(']')).split(" ");
                double score = Double.parseDouble(featureScorePair.substring(featureScorePair.indexOf(']')+1).trim());
                subspace.setFeatures(Arrays.stream(features).map(Integer::parseInt).collect(Collectors.toList()));
                subspace.setScore(score);
                subspacesList.add(subspace);
            }
            pointsRelSubspaces.add(subspacesList);
        }
        return pointsRelSubspaces;
    }

    private String getOutputFilePath(String resultsOutputFilePath, String fileName) {
        Uri uri = new Uri(resultsOutputFilePath);
        String pathToFile = FilenameUtils.getPath(uri.getPath());
        fileName += ".csv";
        return Paths.get(pathToFile, outputFolderName, fileName).toString();
    }

    private double get2DigitPrecision(double num) {
        return Double.parseDouble(df2.format(num));
    }

    private void writeToDisk(DataFrame dataFrame, String dir, String analyticsFileName) throws IOException {
        CSVDataFrameWriter writer = new CSVDataFrameWriter();
        Path analyticsFilePath = Paths.get(getOutputFilePath(dir, analyticsFileName));
        Files.createDirectories(analyticsFilePath.getParent());
        writer.writeToStream(dataFrame, new FileWriter(new File(analyticsFilePath.toString())));
    }

    private static class Subspace {
        private double score;
        private List<Integer> features;

        public Subspace() {
            this.features = new ArrayList<>();
        }

        public Subspace(double score, List<Integer> features) {
            this.score = score;
            this.features = features;
        }

        public void setScore(double score) {
            this.score = score;
        }

        public void setFeatures(List<Integer> features) {
            this.features = features;
        }

        public double getScore() {
            return score;
        }

        public List<Integer> getFeatures() {
            return features;
        }

        public void addFeature(Integer feature) {
            this.features.add(feature);
        }

        @Override
        public String toString() {
            return "[" + Joiner.on(" ").join(features) + "]: " + df2.format(score);
        }
    }

}
