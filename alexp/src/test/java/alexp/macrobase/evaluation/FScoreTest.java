package alexp.macrobase.evaluation;

import org.junit.Test;

import static org.junit.Assert.*;

public class FScoreTest {

    @Test
    public void testFscore() {
        assertEquals(0.6492, new FScore().evaluate(new ConfusionMatrix(485, 515, 9, 991)), 0.001);
    }
}