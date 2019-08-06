package alexp.macrobase.explanation.utils.anomalyDetectorsWrapper;

import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import com.google.common.base.Joiner;
import javafx.util.Pair;
import spark.utils.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;

public class OutlierDetectorsWrapper {

    private static final String pythonCommand = "python";
    private static final String pythonFilePath = "alexp/demo/scripts/anomalyDetectors.py";

    private static final String anomalyDetectorOption = "-ad";
    private static final String paramsOption = "-params";
    private static final String subspaceOption = "-s";
    private static final String subspacesListOption = "-sl";
    private static final String datasetPathOption = "-d";
    private static final String datasetDimOption = "-dim";
    private static final String combinationsOption = "-exhaust";
    private static final String argsFromFileOption = "-args_from_file";
    private static final String classifierRunRepeatOption = "-classifier_run_repeat";

    private static final String pythonResultsDelimiter = " ,\t\n[]{}";
    private static final String subspaceTag = "@subspace";
    private static final String endTag = "@END";

    public static double[] runPythonClassifier(AlgorithmConfig classifierConf, int classifierRunRepeat, String datasetPath,
                                               HashSet<Integer> features, int datasetDim, int sampleSize) throws Exception {
        String algorithmId = classifierConf.getAlgorithmId();
        String params = classifierConf.getParameters().toString().replace(" ", "");
        String subspace = featuresToString(features);
        datasetPath = "\"" + datasetPath + "\"";
        if (!Files.exists(Paths.get(pythonFilePath)))
            throw new IOException("File not found " + pythonFilePath);
        ProcessBuilder pb = new ProcessBuilder(
                pythonCommand,           pythonFilePath,
                anomalyDetectorOption,              algorithmId,
                classifierRunRepeatOption,          "" + classifierRunRepeat,
                paramsOption,                       params,
                subspaceOption,                     subspace,
                datasetDimOption,                   "" + datasetDim,
                datasetPathOption,                  datasetPath
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        double[] pointsScores = parseSingleSubspace(in, sampleSize);
        in.close();
        return pointsScores;
    }

    public static List<Pair<String, double[]>> runPythonClassifierExhaustive(AlgorithmConfig classifierConf, int classifierRunRepeat,
                                                                             String datasetPath, int datasetDim, int combinations,
                                                                             int sampleSize) throws Exception {
        String algorithmId = classifierConf.getAlgorithmId();
        String params = classifierConf.getParameters().toString().replace(" ", "");
        datasetPath = "\"" + datasetPath + "\"";
        if (!Files.exists(Paths.get(pythonFilePath)))
            throw new IOException("File not found " + pythonFilePath);
        ProcessBuilder pb = new ProcessBuilder(
                pythonCommand,              pythonFilePath,
                anomalyDetectorOption,                algorithmId,
                classifierRunRepeatOption,            "" + classifierRunRepeat,
                paramsOption,                         params,
                combinationsOption,                   "" + combinations,
                datasetDimOption,                     "" + datasetDim,
                datasetPathOption,                    datasetPath
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        List<Pair<String, double[]>> subspacePointsScores = parseMultiSubspaces(in, sampleSize);
        in.close();
        return subspacePointsScores;
    }

    public static Map<String, double[]>  runPythonClassifierOnSubspaces(AlgorithmConfig classifierConf, int classifierRunRepeat,
                                                                        String datasetPath, List<HashSet<Integer>> subspacesFeatures,
                                                                        int datasetDim, int sampleSize) throws Exception {
        String classifierId = classifierConf.getAlgorithmId();
        String params = classifierConf.getParameters().toString().replace(" ", "");
        String subspacesAsStr = subspacesToString(subspacesFeatures);
        datasetPath = "\"" + datasetPath + "\"";

        if (!Files.exists(Paths.get(pythonFilePath)))
            throw new IOException("File not found " + pythonFilePath);

        LocalDateTime myObj = LocalDateTime.now();
        String dateTime =
                myObj.toString().replace("-", "").replace(".","").replace(":","");
        String tmpArgsFileName = "tmpArgs_" + dateTime;

        saveArgsToTmpFile(classifierId, classifierRunRepeat, params, subspacesAsStr, datasetPath, datasetDim, tmpArgsFileName);

        ProcessBuilder pb = new ProcessBuilder(
                pythonCommand, pythonFilePath,
                argsFromFileOption, tmpArgsFileName
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        Map<String, double[]> pointsScoresMap = new HashMap<>();
        List<Pair<String, double[]>>  pointsScores = parseMultiSubspaces(in, sampleSize);
        if (pointsScores.size() != subspacesFeatures.size()) {
            throw new RuntimeException("Input and output subspaces in python do not have same size. " +
                    pointsScores.size() + " " + subspacesFeatures.size());
        }
        for(Pair<String, double[]> pair : pointsScores) {
            pointsScoresMap.put(pair.getKey(), pair.getValue());
        }
        in.close();
        File file = new File(tmpArgsFileName);
        file.delete();
        return pointsScoresMap;
    }


    private static double[] parseSingleSubspace(BufferedReader in, int sampleSize) throws IOException {
        String line;
        String consoleMsg = "";
        int counter = 0;
        double[] pointsScores = new double[sampleSize];
        while ((line = in.readLine()) != null) {
            if (line.equals("\n") || StringUtils.isBlank(line)) {
                continue;
            }
            if(!line.startsWith(subspaceTag)) {
                consoleMsg += line;
                continue;
            }
            String[] lineParts = line.split("=");   // Example @subspace [0, 1] = [0.5, 0.4, 0.07...]
            StringTokenizer strtok = new StringTokenizer(lineParts[1], pythonResultsDelimiter);
            while (strtok.hasMoreElements()) {
                double num = Double.parseDouble(strtok.nextToken());
                pointsScores[counter++] = num;
            }
        }
        if (counter < sampleSize) {
            System.out.println(consoleMsg);
            throw new RuntimeException("Error occurred in python. See the console message above");
        }
        return pointsScores;
    }

    private static List<Pair<String, double[]>> parseMultiSubspaces(BufferedReader in, int sampleSize) throws IOException {
        List<Pair<String, double[]>> subspacePointsScores = new ArrayList<>();
        String line;
        String consoleMsg = "";
        double global_min = 0;
        while ((line = in.readLine()) != null) {
            int counter = 0;
            double[] pointsScores = new double[sampleSize];
            if (line.equals("\n") || StringUtils.isBlank(line)) {
                continue;
            }
            if(line.startsWith(">")) {
                System.out.print('\r' + line);
                continue;
            }
            if(!line.startsWith(subspaceTag)) {
                consoleMsg += line;
                continue;
            }
            String[] lineParts = line.split("=");
            String subspace = lineParts[0].replace("@subspace", "").trim();
            StringTokenizer strtok = new StringTokenizer(lineParts[1], pythonResultsDelimiter);
            while (strtok.hasMoreElements()) {
                double num = Double.parseDouble(strtok.nextToken());
                pointsScores[counter++] = num;
                if (num < global_min) {
                    global_min = num;
                }
            }
            subspacePointsScores.add(new Pair<>(subspace, pointsScores));
            if (counter < sampleSize) {
                System.out.println(consoleMsg);
                throw new RuntimeException("Error occurred in python. See the console message above");
            }
        }
        if (subspacePointsScores.isEmpty()) {
            System.out.println("\n" + consoleMsg);
            throw new RuntimeException("Error occurred in python. See the console message above");
        }
        if (global_min < 0) {
            rescaleNegativeNumbers(subspacePointsScores, global_min);
        }
        return subspacePointsScores;
    }

    private static void saveArgsToTmpFile(String algorithmId, int classifierRunRepeat, String params, String subspacesAsStr,
                                          String datasetPath, int datasetDim, String tmpArgsFileName) throws IOException{
        File file = new File(tmpArgsFileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        String args =
                    anomalyDetectorOption       + " " +     algorithmId                         + " "
                +   classifierRunRepeatOption   + " " +     classifierRunRepeat                 + " "
                +   paramsOption                + " " +     params                              + " "
                +   subspacesListOption         + " " +     subspacesAsStr                      + " "
                +   datasetPathOption           + " " +     replaceSpacesInPath(datasetPath)    + " "
                +   datasetDimOption            + " " +     datasetDim;
        bw.write(args);
        bw.close();
    }

    private static String replaceSpacesInPath(String datasetPath) {
        return datasetPath.replace(" ", "%20");
    }

    private static String featuresToString(HashSet<Integer> features) {
        return "[" + Joiner.on(",").join(features) + "]";
    }

    private static String subspacesToString(List<HashSet<Integer>> subspacesFeatures) {
        StringBuilder sb = new StringBuilder();
        for (HashSet<Integer> features : subspacesFeatures) {
            sb.append(featuresToString(features)).append(";");
        }
        return sb.toString();
    }

    private static void rescaleNegativeNumbers(List<Pair<String, double[]>> subspacePointsScores, double global_min) {
        if (global_min >= 0) {
            throw new RuntimeException("global_min should be < 0 (" + global_min + ")");
        }
        for (int sub_num = 0; sub_num < subspacePointsScores.size(); sub_num++) {
            Pair<String, double[]> pair = subspacePointsScores.get(sub_num);
            double[] scores = pair.getValue();
            String subspace = pair.getKey();
            for (int score_num = 0; score_num < scores.length; score_num++) {
                scores[score_num] = scores[score_num] - global_min;
            }
            subspacePointsScores.set(sub_num, new Pair<>(subspace, scores));
        }
    }

}
