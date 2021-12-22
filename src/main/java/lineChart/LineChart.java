package lineChart;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.ui.ApplicationFrame;

import java.awt.*;

public class LineChart extends ApplicationFrame
{
    private static final long serialVersionUID = 1L;

    public LineChart(final String title, Dataset dates) {
        super(title);
        final CategoryDataset dataset    = dates.createDataset();
        final JFreeChart chart      = createChart(dataset);
        final ChartPanel chartPanel = new ChartPanel(chart);

        chartPanel.setPreferredSize(new Dimension(1000, 600));
        setContentPane(chartPanel);
    }

    private JFreeChart createChart(final CategoryDataset dataset)
    {
        final JFreeChart chart = ChartFactory.createLineChart(
                "TASK 1",             // chart title
                null,                      // domain axis label
                "Значение",                // range axis label
                dataset,                   // data
                PlotOrientation.VERTICAL,  // orientation
                true,                      // include legend
                true,                      // tooltips
                false                      // urls
        );

        chart.setBackgroundPaint(Color.white);

        final CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.lightGray);
        plot.setRangeGridlinePaint(Color.white);

        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(
                NumberAxis.createIntegerTickUnits());
        rangeAxis.setAutoRangeIncludesZero(true);

        LineAndShapeRenderer renderer;
        renderer = (LineAndShapeRenderer) plot.getRenderer();

        renderer.setSeriesStroke(
                0, new BasicStroke(2.0f, BasicStroke.CAP_ROUND,
                        BasicStroke.JOIN_ROUND,
                        1.0f, new float[] {10.0f, 6.0f}, 0.0f)
        );
        renderer.setSeriesStroke(
                1, new BasicStroke(2.0f, BasicStroke.CAP_ROUND,
                        BasicStroke.JOIN_ROUND,
                        1.0f, new float[] {6.0f, 6.0f}, 0.0f)
        );
        return chart;
    }

}