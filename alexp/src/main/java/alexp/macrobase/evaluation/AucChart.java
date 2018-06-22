package alexp.macrobase.evaluation;

import alexp.macrobase.evaluation.roc.Curve;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import java.io.File;
import java.io.IOException;

public class AucChart {
    public void saveToPng(Curve aucCurve, String filePath) throws IOException {
        double[][] points = aucCurve.rocPoints();

        final XYSeries series = new XYSeries("AUC");
        for (double[] p : points) {
            series.add(p[0],p[1]);

        }

        final XYSeriesCollection data = new XYSeriesCollection(series);

        final JFreeChart chart = ChartFactory.createXYLineChart(
                "ROC AUC for " + new File(filePath).getName().replace(".png", ""),
                "False Positive Rate",
                "True Positive Rate",
                data,
                PlotOrientation.VERTICAL,
                false,
                false,
                false
        );

        new File(filePath).getParentFile().mkdirs();

        ChartUtils.saveChartAsPNG(new File(filePath), chart, 800, 600);
    }
}
