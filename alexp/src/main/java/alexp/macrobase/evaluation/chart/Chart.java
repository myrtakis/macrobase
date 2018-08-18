package alexp.macrobase.evaluation.chart;

import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;

import java.io.File;
import java.io.IOException;

public abstract class Chart<T extends Chart<T>> {
    protected String name;

    protected JFreeChart chart;

    public void saveToPng(String filePath) throws IOException {
        new File(filePath).getParentFile().mkdirs();

        ChartUtils.saveChartAsPNG(new File(filePath), chart, 800, 600);
    }

    public String getName() {
        return name;
    }

    public T setName(String name) {
        this.name = name;
        return self();
    }

    protected abstract T self();

}
