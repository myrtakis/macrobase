package alexp.macrobase.utils;

import alexp.macrobase.evaluation.roc.Curve;
import com.google.common.base.Stopwatch;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class BenchmarkUtils {
    public static long measureTime(ThrowingRunnable f) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();

        f.run();

        return sw.elapsed(TimeUnit.MILLISECONDS);
    }

    public static Curve aucCurve(double[] classifierResult, int[] labels) {
        return new Curve.PrimitivesBuilder()
                .scores(classifierResult)
                .labels(labels)
                .build();
    }
}
