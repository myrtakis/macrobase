package alexp.macrobase.evaluation;

public class FScore {
    private final double b; // https://en.wikipedia.org/wiki/F1_score#Formulation

    public FScore() {
        this(1);
    }

    public FScore(double b) {
        this.b = b;
    }

    public double evaluate(ConfusionMatrix cn) {
        return (1 + b * b) * ((cn.precision() * cn.recall()) / ((b * b * cn.precision()) + cn.recall()));
    }
}
