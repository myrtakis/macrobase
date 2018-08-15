package alexp.macrobase.evaluation.chart;

public class ResultPoint {
    private final double time;
    private final double value;
    private final double score;
    private final double threshold;
    private final boolean label;

    public ResultPoint(double time, double value, double score, double threshold, boolean label) {
        this.time = time;
        this.value = value;
        this.score = score;
        this.threshold = threshold;
        this.label = label;
    }

    public double getTime() {
        return time;
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

    public boolean isLabel() {
        return label;
    }
}
