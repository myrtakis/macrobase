package alexp.macrobase.evaluation;

public class Accuracy {
    public double evaluate(ConfusionMatrix cn) {
        return (cn.truePositive() + cn.trueNegative()) / (double) cn.totalCount();
    }
}
