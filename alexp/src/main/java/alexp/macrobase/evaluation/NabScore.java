package alexp.macrobase.evaluation;

public class NabScore {
    private double tpWeight = 1.0;
    private double fpWeight = 0.11;
    private double fnWeight = 1.0;

    public double evaluate(boolean[] results, int[] labels) {
        // crude version, without positions/sigmoid

        double tpScore = 0, fpScore = 0, fnScore = 0;
        boolean windowHandled = false;

        for (int i = results.length / 10; i < results.length; i++) {
            if (results[i]) {
                if (labels[i] == 1 && !windowHandled) {
                    tpScore += tpWeight;
                    windowHandled = true;
                }
                if (labels[i] == 0) {
                    fpScore -= fpWeight;
                }
            }

            if (labels[i] == 0 && labels[i - 1] == 1) {
                if (!windowHandled) {
                    fnScore -= fnWeight;
                }
                windowHandled = false;
            }
        }

        return tpScore + fpScore + fnScore;
    }

    public double getTpWeight() {
        return tpWeight;
    }

    public NabScore setTpWeight(double tpWeight) {
        this.tpWeight = tpWeight;
        return this;
    }

    public double getFpWeight() {
        return fpWeight;
    }

    public NabScore setFpWeight(double fpWeight) {
        this.fpWeight = fpWeight;
        return this;
    }

    public double getFnWeight() {
        return fnWeight;
    }

    public NabScore setFnWeight(double fnWeight) {
        this.fnWeight = fnWeight;
        return this;
    }

    private static double sigmoid(double x) {
        return 1.0 / (1.0 + Math.exp(-x));
    }

    /**
     * https://github.com/numenta/NAB/blob/master/nab/scorer.py#L259
     * Return a scaled sigmoid function given a relative position within a
     *   labeled window.  The function is computed as follows:
     *   A relative position of -1.0 is the far left edge of the anomaly window and
     *   corresponds to S = 2*sigmoid(5) - 1.0 = 0.98661.  This is the earliest to be
     *   counted as a true positive.
     *   A relative position of -0.5 is halfway into the anomaly window and
     *   corresponds to S = 2*sigmoid(0.5*5) - 1.0 = 0.84828.
     *   A relative position of 0.0 consists of the right edge of the window and
     *   corresponds to S = 2*sigmoid(0) - 1 = 0.0.
     *   Relative positions > 0 correspond to false positives increasingly far away
     *   from the right edge of the window. A relative position of 1.0 is past the
     *   right  edge of the  window and corresponds to a score of 2*sigmoid(-5) - 1.0 =
     *   -0.98661.
     */
    private static double scaledSigmoid(double relativePositionInWindow ) {
        if (relativePositionInWindow > 3.0) {
            return -1.0;
        }
        return 2 * sigmoid(-5 * relativePositionInWindow) - 1.0;
    }
}
