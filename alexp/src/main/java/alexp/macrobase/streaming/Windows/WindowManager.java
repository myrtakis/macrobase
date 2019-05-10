package alexp.macrobase.streaming.Windows;

import alexp.macrobase.pipeline.benchmark.config.AlgorithmConfig;
import alexp.macrobase.pipeline.benchmark.config.DatasetConfig;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;


public class WindowManager {

    private final String windowTypeKey = "windowType"; // must be either "sliding" or "tumbling"
    private final String windowModeKey = "windowMode"; // must be either "count" or "time"
    private final String windowSizeKey = "windowSize"; // must be an integer
    private final String windowSlideKey = "windowSlide"; // must be an integer

    private AlgorithmConfig classifierConf;
    private DatasetConfig datasetConf;

    private String windowMode;
    private String windowType;
    private int windowSize;
    private int windowSlide;
    private int windowSizeTime;
    private int windowSlideTime;
    private Window window;

    public WindowManager(AlgorithmConfig classifierConf, DatasetConfig datasetConf) {
        this.classifierConf = classifierConf;
        this.datasetConf = datasetConf;
        this.windowSize = classifierConf.getParameters().get(windowSizeKey);
        if (classifierConf.getParameters().get(windowSlideKey) != null) {
            this.windowSlide = classifierConf.getParameters().get(windowSlideKey);
        }
        this.windowSizeTime = classifierConf.getParameters().get(windowSizeKey);
        if (classifierConf.getParameters().get(windowSlideKey) != null) {
            this.windowSlideTime = classifierConf.getParameters().get(windowSlideKey);
        }
        this.windowMode = classifierConf.getParameters().get(windowModeKey);
        this.windowType = classifierConf.getParameters().get(windowTypeKey);
        initWindowModeSettings(false);
        initWindowMethod();
    }

    private void initWindowMethod() {
        switch (this.windowType) {
            case "tumbling":
                window = new Tumbling(windowSize);
                break;
            case "sliding":
                window = new Sliding(windowSize, windowSlide);
                break;
            default:
                throw new IllegalArgumentException("window type is unknown! " + windowTypeKey + " You must select between 'sliding' or 'tumbling' window");
        }
    }

    public String getWindowMethod() {
        String windowMethod = classifierConf.getParameters().get(windowTypeKey);
        if (windowMethod.isEmpty()) {
            return "none";
        } else {
            return windowMethod.toLowerCase();
        }
    }

    public void manage(String rawDp) {
        window.build(rawDp);
    }

    public boolean windowIsConstructed() {
        return window.windowIsReady();
    }

    public boolean isEndStream() {
        return window.isEndStream();
    }


    public DataFrame getWindowDF() throws Exception {
        // INITIALIZE THE DATA FRAME
        DataFrame dataFrame = new DataFrame();
        // GET ALL THE DATASET COLUMNS
        List<String> dimensions = new ArrayList<>();
        for (String dim : datasetConf.getMetricColumns()) {
            dimensions.add(dim);
        }
        dimensions.add(datasetConf.getLabelColumn());
        // GET ALL WINDOW DATA
        List<String> windowRawDP = window.getWindow();
        Map<String, double[]> accumulator = new HashMap<>();
        int dimCounter = 0;
        int pointCounter = 0;
        System.out.println("==================== START");
        for (String dp : windowRawDP) {
            System.out.println(dp);
            String[] lineParts = dp.split("[,]\\s+");
            for (String numStr : lineParts) {
                double num = Double.parseDouble(numStr);
                String currLabel = dimensions.get(dimCounter);
                double[] tmpTable = accumulator.getOrDefault(currLabel, new double[windowRawDP.size()]);
                tmpTable[pointCounter] = num;
                accumulator.put(currLabel, tmpTable);
                dimCounter++;
            }
            pointCounter++;
            dimCounter = 0;
        }
        System.out.println("==================== END");
        // BUILD THE DATA FRAME
        for (String dimension : accumulator.keySet()) {
            dataFrame.addColumn(dimension, accumulator.get(dimension));
        }
        // RETURN THE DATA FRAME
        return dataFrame;
    }

    public void clearWindowData() {
        window.clearWindow();
        initWindowModeSettings(true);
    }

    private void initWindowModeSettings(boolean updateWindowInstance) {
        switch (this.windowMode) {
            case "time":
                int maxBound = 5; // maximum number of points bound
                int minBound = 1; // minimum number of points bound
                this.windowSize = new Random().nextInt(this.windowSizeTime * maxBound) + minBound;
                int shift = (int) Math.ceil(((double) this.windowSlideTime / this.windowSizeTime) * this.windowSize);
                this.windowSlide = new Random().nextInt(shift) + 1;
                if (updateWindowInstance) {
                    window.resetParams(new int[]{this.windowSize, this.windowSlide});
                }
                break;
            case "count":
                break;
            default:
                throw new IllegalArgumentException("window mode is unknown! " + windowModeKey + " You must select between 'count' or 'time' based window");
        }

        System.out.println("Window Size: " + this.windowSize);
        System.out.println("Window Slide: " + this.windowSlide);

    }

    public int getWindowSize() {
        return window.getWindow().size();
    }

}