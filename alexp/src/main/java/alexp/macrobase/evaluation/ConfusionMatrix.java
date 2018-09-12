package alexp.macrobase.evaluation;

import java.util.Objects;

public class ConfusionMatrix {
    private int tp = 0, fp = 0, fn = 0, tn = 0;

    public ConfusionMatrix(double[] scores, int[] labels, double threshold) {
        if (scores.length != labels.length) {
            throw new IllegalArgumentException("Labels count doesn't match scores count");
        }

        for (int i = 0; i < scores.length; i++) {
            final boolean result = scores[i] > threshold;
            final boolean label = labels[i] == 1;

            if (result) {
                if (label) {
                    tp++;
                } else {
                    fp++;
                }
            } else { // !result
                if (label) {
                    fn++;
                } else {
                    tn++;
                }
            }
        }
    }

    public ConfusionMatrix(int tp, int fp, int fn, int tn) {
        this.tp = tp;
        this.fp = fp;
        this.fn = fn;
        this.tn = tn;
    }

    public int truePositive() {
        return tp;
    }

    public int falsePositive() {
        return fp;
    }

    public int falseNegative() {
        return fn;
    }

    public int trueNegative() {
        return tn;
    }

    public int totalCount() {
        return truePositive() + falsePositive() + falseNegative() + trueNegative();
    }

    public int positiveCount() {
        return tp + fp;
    }

    public int negativeCount() {
        return tn + fn;
    }

    public double precision() {
        return truePositive() / ((double) truePositive() + falsePositive());
    }

    public double recall() {
        return truePositive() / ((double) truePositive() + falseNegative());
    }

    @Override
    public String toString() {
        return String.format("True Positive %d, False Positive %d, False Negative %d, True Negative %d",
                truePositive(), falsePositive(), falseNegative(), trueNegative());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfusionMatrix that = (ConfusionMatrix) o;
        return truePositive() == that.truePositive() &&
                falsePositive() == that.falsePositive() &&
                falseNegative() == that.falseNegative() &&
                trueNegative() == that.trueNegative();
    }

    @Override
    public int hashCode() {

        return Objects.hash(truePositive(), falsePositive(), falseNegative(), trueNegative());
    }
}
