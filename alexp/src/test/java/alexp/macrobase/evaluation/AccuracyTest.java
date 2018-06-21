package alexp.macrobase.evaluation;

import org.junit.Test;

import static org.junit.Assert.*;

public class AccuracyTest {

    @Test
    public void evaluate() {
        Accuracy acc = new Accuracy();
        assertEquals(0.4, acc.evaluate(new ConfusionMatrix(
                new double[] { 0, 1, 1, 0, 0, 0, 1, 1, 0, 1 },
                new int[] {    1, 0, 1, 0, 0, 0, 0, 0, 1, 0}, 0.5)), 0.001);
    }
}