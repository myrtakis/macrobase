package alexp.macrobase.evaluation.chart;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.awt.*;
import java.util.List;

public class AnomalyDataChart extends Chart<AnomalyDataChart> {
    public AnomalyDataChart createAnomaliesChart(List<ResultPoint> points) {
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

    @Override
    protected AnomalyDataChart self() {
        return this;
    }
}
