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
        double[][] prPoints = aucCurve.prPoints();

        final XYSeries rocSeries = new XYSeries("ROC AUC");
        for (double[] p : points) {
            rocSeries.add(p[0],p[1]);
        }

        final XYSeries prSeries = new XYSeries("PR AUC");
        for (double[] p : prPoints) {
            prSeries.add(p[0],p[1]);
        }

        final XYSeries f1Series = new XYSeries("F1-score");
        final XYSeries accuracySeries = new XYSeries("Accuracy");
        FScore fScore = new FScore();
        Accuracy accuracy = new Accuracy();
        for (int i = 0; i < points.length; i++) {
            ConfusionMatrix matr = aucCurve.confusionMatrix(i);
            f1Series.add(points[i][0], fScore.evaluate(matr));
            accuracySeries.add(points[i][0], accuracy.evaluate(matr));
        }

        final XYSeriesCollection data = new XYSeriesCollection();
        data.addSeries(rocSeries);
        data.addSeries(prSeries);
        data.addSeries(f1Series);
        data.addSeries(accuracySeries);

        final JFreeChart chart = ChartFactory.createXYLineChart(
                new File(filePath).getName().replace(".png", ""),
                "False Positive Rate",
                "True Positive Rate | F1-score | Accuracy",
                data,
                PlotOrientation.VERTICAL,
                true,
                false,
                false
        );

        new File(filePath).getParentFile().mkdirs();

        ChartUtils.saveChartAsPNG(new File(filePath), chart, 800, 600);
    }
}
