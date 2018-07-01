package alexp.macrobase.evaluation;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class GridSearchTest {

    @Test
    public void runTest() throws Exception {
        GridSearch gs = new GridSearch()
                .addParam("p1", new Double[] { 1.0, 2.0, 3.0 })
                .addParam("p2", new Integer[] { 4, 5 })
                .addParam("p3", new String[] { "on", "off" });

        AtomicInteger c = new AtomicInteger();
        gs.run(p -> c.getAndIncrement());

        assertEquals(new HashSet<>(
                Arrays.asList(
                        ImmutableMap.of("p1", 1.0, "p2", 4, "p3", "on"),
                        ImmutableMap.of("p1", 1.0, "p2", 4, "p3", "off"),
                        ImmutableMap.of("p1", 1.0, "p2", 5, "p3", "on"),
                        ImmutableMap.of("p1", 1.0, "p2", 5, "p3", "off"),
                        ImmutableMap.of("p1", 2.0, "p2", 4, "p3", "on"),
                        ImmutableMap.of("p1", 2.0, "p2", 4, "p3", "off"),
                        ImmutableMap.of("p1", 2.0, "p2", 5, "p3", "on"),
                        ImmutableMap.of("p1", 2.0, "p2", 5, "p3", "off"),
                        ImmutableMap.of("p1", 3.0, "p2", 4, "p3", "on"),
                        ImmutableMap.of("p1", 3.0, "p2", 4, "p3", "off"),
                        ImmutableMap.of("p1", 3.0, "p2", 5, "p3", "on"),
                        ImmutableMap.of("p1", 3.0, "p2", 5, "p3", "off")
                )
        ), new HashSet<>(gs.getResults().values()));
        assertEquals(0.0, gs.getResults().firstKey(), 0.001);
        assertEquals(11.0, gs.getResults().lastKey(), 0.001);
    }
}