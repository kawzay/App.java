package lineChart;

import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.util.Map;
import java.util.SortedMap;

public class Dataset
{
    private SortedMap<String, Double> dataValues;

    public Dataset(SortedMap<String,Double> dataValues){

        this.dataValues = dataValues;
    }

    public CategoryDataset createDataset() {
        DefaultCategoryDataset dataset;

        final String series1 = "Series 1" ;
        dataset = new DefaultCategoryDataset();

        for (Map.Entry<String, Double> point : dataValues.entrySet()){

            dataset.addValue(point.getValue(), series1, point.getKey());
        }

        return dataset;
    }

}
