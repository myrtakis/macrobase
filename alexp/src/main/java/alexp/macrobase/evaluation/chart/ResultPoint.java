package alexp.macrobase.evaluation.chart;

public class ResultPoint {
    private final double time;
    private final String timeStr;
    private final double value;
    private final double score;
    private double threshold;
    private final boolean label;

    public ResultPoint(double time, String timeStr, double value, double score, double threshold, boolean label) {
        this.time = time;
        this.timeStr = timeStr;
        this.value = value;
        this.score = score;
        this.threshold = threshold;
        this.label = label;
    }

    public double getTime() {
        return time;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public double getValue() {
        return value;
    }

    public double getScore() {
        return score;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public boolean isLabel() {
        return label;
    }
}
