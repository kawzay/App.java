import dao.SchoolCSV;
import lineChart.Dataset;
import lineChart.LineChart;
import org.jfree.ui.RefineryUtilities;
import query.DbWorker;

import java.util.*;

public class App {
    public static void main(String[] args) {

        Locale.setDefault(new Locale("en", "US"));
        List<String> countysForTask2 = Arrays.asList("Fresno", "Contra Costa","El Dorado","Glenn");
        if(SchoolCSV.openFile()){
            DbWorker worker = new DbWorker();

            worker.dropSchoolTable();
            worker.createSchoolTable();
            worker.addWithCSV();
            System.out.println("TASK 1");
            SortedMap<String, Double> test = worker.taskFirst();
            Dataset dataset = new Dataset(test);
            final LineChart view = new LineChart("TASK 1", dataset);
            view.pack();
            RefineryUtilities.centerFrameOnScreen(view);
            view.setVisible(true);

            System.out.println("TASK 2");
            worker.taskSecond(countysForTask2, 10);
            System.out.println("TASK 3");
            worker.thirdTask(5000,  7500);
            worker.thirdTask(10000,  11000);
            worker.stop();
        } else{
            System.out.println("File .csv not open!");
        }




    }

}
