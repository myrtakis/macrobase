package alexp.macrobase.explanation.utils;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplanationUtils {

    private static String pointIdColumn = "PointId";
    private static String avgPointScore = "AvgScore";

    public static void isolatePointsOfInterestDF (String outputFilePath, DataFrame results, String outputColumnName,
                                                  String relSubspaceColumnName, List<Integer> pointsOfInterest) throws IOException {
        DataFrame data = new DataFrame();
        double[] poiIDs = new double[pointsOfInterest.size()];
        double[] poiScores = new double[pointsOfInterest.size()];
        String[] poiRelSubspaces = new String[pointsOfInterest.size()];

        boolean outputColIsIncluded = results.getSchema().getColumnNames().contains(outputColumnName);
        boolean relSubspaceColIsIncluded = results.getSchema().getColumnNames().contains(relSubspaceColumnName);
        int counter = 0;
        for(int pointId : pointsOfInterest){
            poiIDs[counter] = pointId;
            poiScores[counter] = results.getRow(pointId).getAs(outputColumnName);
            poiRelSubspaces[counter] = results.getRow(pointId).getAs(relSubspaceColumnName);
            counter++;
        }
        data.addColumn(pointIdColumn, poiIDs);
        data.addColumn(avgPointScore, poiScores);
        data.addColumn(relSubspaceColumnName, poiRelSubspaces);
        CSVDataFrameWriter writer = new CSVDataFrameWriter();
        writer.writeToStream(data, new FileWriter(new File(outputFilePath)));
    }

    public static void isolatePointsOfInterestFromCSV (String inputFilePath, String outputFilePath, String outputColumnName,
                                                String relSubspaceColumnName, List<Integer> pointsOfInterest) throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<String, Schema.ColType>(){
            {
                put(outputColumnName, Schema.ColType.DOUBLE);
                put(relSubspaceColumnName, Schema.ColType.STRING);
            }
        };
        List<String> requiredColumns = new ArrayList<>(colTypes.keySet());
        DataFrame dataFrame = PipelineUtils.loadDataFrame(inputFilePath, colTypes, requiredColumns);
        isolatePointsOfInterestDF(outputFilePath, dataFrame, outputColumnName, relSubspaceColumnName, pointsOfInterest);
    }

}
