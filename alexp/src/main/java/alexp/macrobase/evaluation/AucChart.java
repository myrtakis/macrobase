package alexp.macrobase.evaluation;

import alexp.macrobase.evaluation.roc.Curve;
import com.google.common.collect.Streams;
import joptsimple.internal.Strings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AucChart {
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
            Accuracy accuracy = new Accuracy();
            double[][] points = aucCurve.rocPoints();

            final XYSeries series = new XYSeries("Accuracy");
            for (int i = 0; i < points.length; i++) {
                ConfusionMatrix matr = aucCurve.confusionMatrix(i);
                series.add(points[i][0], accuracy.evaluate(matr));
            }

            return series;
        }, "Accuracy");

    }

    private String name;

    private JFreeChart chart;

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

    public AucChart createAnomaliesChart(List<ResultPoint> points) {
        final XYSeriesCollection data = new XYSeriesCollection();

        final XYSeries normalSeries = new XYSeries("Normal values (TN)");
        final XYSeries labelSeries = new XYSeries("Undetected anomalies (FN)");
        final XYSeries correctResultSeries = new XYSeries("Correctly detected anomalies (TP)");
        final XYSeries wrongResultSeries = new XYSeries("Incorrectly detected anomalies (FP)");

        points.forEach(p -> {
            if (p.isLabel()) {
                if (p.getScore() > p.getThreshold()) {
                    correctResultSeries.add(p.getTime(), p.getValue());
                } else {
                    labelSeries.add(p.getTime(), p.getValue());
                }
            } else {
                if (p.getScore() > p.getThreshold()) {
                    wrongResultSeries.add(p.getTime(), p.getValue());
                } else {
                    normalSeries.add(p.getTime(), p.getValue());
                }
            }
        });

        data.addSeries(normalSeries);
        data.addSeries(labelSeries);
        data.addSeries(correctResultSeries);
        data.addSeries(wrongResultSeries);

        chart = ChartFactory.createScatterPlot(
                name,
                "Time",
                "Value",
                data,
                PlotOrientation.VERTICAL,
                true,
                false,
                false
        );

        XYItemRenderer renderer = ((XYPlot) chart.getPlot()).getRenderer();
        renderer.setSeriesPaint(0, new Color(123, 156, 255));
        renderer.setSeriesPaint(1, new Color(255, 0, 0));
        renderer.setSeriesPaint(2, new Color(0, 255, 0));
        renderer.setSeriesPaint(3, new Color(255, 216, 51));
        for (int i = 0; i < data.getSeries().size(); i++) {
            renderer.setSeriesShape(i, renderer.getDefaultShape());
        }

        return this;
    }

    public void saveToPng(String filePath) throws IOException {
        new File(filePath).getParentFile().mkdirs();

        ChartUtils.saveChartAsPNG(new File(filePath), chart, 800, 600);
    }

    public String getName() {
        return name;
    }

    public AucChart setName(String name) {
        this.name = name;
        return this;
    }
}
