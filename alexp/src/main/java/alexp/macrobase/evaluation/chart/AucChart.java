package alexp.macrobase.evaluation.chart;

import alexp.macrobase.evaluation.Accuracy;
import alexp.macrobase.evaluation.ConfusionMatrix;
import alexp.macrobase.evaluation.FScore;
import alexp.macrobase.evaluation.roc.Curve;
import com.google.common.collect.Streams;
import joptsimple.internal.Strings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AucChart extends Chart<AucChart> {
    @FunctionalInterface
    public interface SeriesSupplier extends Function<Curve, XYSeries> {
    }

    public static class Measure {
        public final SeriesSupplier seriesSupplier;
        public final String name;
        public final String yLabel;

        public Measure(SeriesSupplier seriesSupplier, String name) {
            this(seriesSupplier, name, name);
        }

        public Measure(SeriesSupplier seriesSupplier, String name, String yLabel) {
            this.seriesSupplier = seriesSupplier;
            this.name = name;
            this.yLabel = yLabel;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class Measures {
        public static Measure RocAuc = new Measure(aucCurve -> {
            double[][] points = aucCurve.rocPoints();

            final XYSeries series = new XYSeries("ROC AUC");
            for (double[] p : points) {
                series.add(p[0],p[1]);
            }

            return series;
        }, "ROC AUC", "True Positive Rate");

        public static Measure PrAuc = new Measure(aucCurve -> {
            double[][] points = aucCurve.prPoints();

            final XYSeries series = new XYSeries("PR AUC");
            for (double[] p : points) {
                series.add(p[0],p[1]);
            }

            return series;
        }, "PR AUC", "Precision");

        public static Measure F1 = new Measure(aucCurve -> {
            double[][] points = aucCurve.rocPoints();
            FScore fScore = new FScore();

            final XYSeries series = new XYSeries("F1-score");
            for (int i = 0; i < points.length; i++) {
                ConfusionMatrix matr = aucCurve.confusionMatrix(i);
                series.add(points[i][0], fScore.evaluate(matr));
            }

            return series;
        }, "F1-score");

        public static Measure Accuracy = new Measure(aucCurve -> {
            alexp.macrobase.evaluation.Accuracy accuracy = new Accuracy();
            double[][] points = aucCurve.rocPoints();

            final XYSeries series = new XYSeries("Accuracy");
            for (int i = 0; i < points.length; i++) {
                ConfusionMatrix matr = aucCurve.confusionMatrix(i);
                series.add(points[i][0], accuracy.evaluate(matr));
            }

            return series;
        }, "Accuracy");

    }

    public AucChart createForSingle(Curve aucCurve, Measure... measures) {
        final XYSeriesCollection data = new XYSeriesCollection();
        for (Measure measure : measures) {
            data.addSeries(measure.seriesSupplier.apply(aucCurve));
        }

        chart = ChartFactory.createXYLineChart(
                name,
                "False Positive Rate",
                Strings.join(Arrays.stream(measures).map(m -> m.yLabel).collect(Collectors.toList()), " | "),
                data,
                PlotOrientation.VERTICAL,
                true,
                false,
                false
        );

        return this;
    }

    public AucChart createForAll(List<Curve> aucCurves, List<String> classifierNames, Measure measure) {
        final XYSeriesCollection data = new XYSeriesCollection();
        for (Pair<String, Curve> item : Streams.zip(classifierNames.stream(), aucCurves.stream(), ImmutablePair::new).collect(Collectors.toList())) {
            XYSeries series = measure.seriesSupplier.apply(item.getValue());
            series.setKey(item.getKey());
            data.addSeries(series);
        }

        chart = ChartFactory.createXYLineChart(
                name,
                "False Positive Rate",
                measure.yLabel,
                data,
                PlotOrientation.VERTICAL,
                true,
                false,
                false
        );

        return this;
    }

    @Override
    protected AucChart self() {
        return this;
    }
}
