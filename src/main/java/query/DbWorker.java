package query;

import connection.DbConnection;
import dao.SchoolCSV;
import entity.School;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class DbWorker {

    public static String creatQuery =
            "CREATE TABLE school (\n" +
            "    id INTEGER PRIMARY KEY,\n" +
            "    district INTEGER NOT NULL,\n" +
            "    school TEXT NOT NULL,\n" +
            "    county TEXT NOT NULL,\n" +
            "    grades TEXT NOT NULL,\n" +
            "    students INTEGER NOT NULL,\n" +
            "    teachers REAL NOT NULL,\n" +
            "    calworks REAL NOT NULL,\n" +
            "    lunch REAL NOT NULL,\n" +
            "    computer INTEGER NOT NULL,\n" +
            "    expenditure REAL NOT NULL,\n" +
            "    income REAL NOT NULL,\n" +
            "    english REAL NOT NULL,\n" +
            "    read REAL NOT NULL,\n" +
            "    math REAL NOT NULL\n" +
            ");";

    public static String taskFirstQuery =
            "SELECT county, AVG(students)\n" +
            "FROM school\n" +
            "GROUP BY county\n" +
            "ORDER BY county \n" +
            "LIMIT 10;";

    public static String taskSecondQuery =
            "SELECT county,AVG(expenditure) \n" +
            "FROM school \n" +
            "WHERE county IN (%s) AND income > %d  \n" +
            "GROUP BY county; ";

    public static String taskThirdQuery =
            "SELECT school\n" +
            "FROM school\n" +
            "WHERE students > %d\n" +
            "AND students < %d\n" +
            "ORDER BY math desc\n" +
            "LIMIT 1;";

    private Connection con;
    private Statement statement;

    public DbWorker(){
        try{
            this.con = DbConnection.getInstance().getConnection();
            this.statement = this.con.createStatement();
        } catch (SQLException e){
            System.out.println("Something went wrong");
        }
    }

    public void addWithCSV(){
        try {
            for (School s : SchoolCSV.schools) {

                String query = String.format("INSERT INTO school VALUES(" +
                                "%d,%d,'%s','%s','%s',%d,%f,%f,%f,%d,%f,%f,%f,%f,%f" +
                                ");",
                        s.getId(), s.getDistrict(), s.getName(), s.getCounty(),s.getGrades(), s.getStudents(),
                        s.getTeachers(),s.getCalworks(),s.getLunch(),s.getComputers(),s.getExpenditure(),
                        s.getIncome(),s.getEnglish(),s.getRead(),s.getMath());
                this.statement.executeUpdate(query);

            }
            System.out.println("All lines added!");
        } catch (SQLException e){
            System.out.println("something went wrong!!");
            e.printStackTrace();
        }

    }

    public void dropSchoolTable(){
        try{
           String query = "DROP TABLE IF EXISTS school";
           this.statement.executeUpdate(query);
            System.out.println("Table droped");
        } catch (SQLException e){
            System.out.println("something went wrong, drop");
        }
    }

    public void createSchoolTable(){
        try{
            String query = creatQuery;
            this.statement.executeUpdate(query);
            System.out.println("Table created");
        } catch (SQLException e){
            System.out.println("something went wrong, drop");
        }
    }

    public TreeMap<String, Double> taskFirst(){

        String query = taskFirstQuery;

        TreeMap<String, Double> resultMap = new TreeMap<>();
        try {
            ResultSet result = this.statement.executeQuery(query);
            while(result.next()){
                resultMap.put(result.getString(1), result.getDouble(2));
                System.out.printf("%s: avg(students) = %f\n", result.getString(1), result.getDouble(2));
            }
        } catch (SQLException e){
            System.out.println("something went wrong, task two");
        }
        return resultMap;
    }

    public void taskSecond(List<String> county, int income){
        String countys = String.join("','",county);
        countys = "'" + countys + "'";
        String query = String.format(taskSecondQuery, countys, income);
        try {
            ResultSet result = this.statement.executeQuery(query);
            while(result.next()){
                System.out.printf("%s: avg(expenditure) = %f\n",
                        result.getString(1), result.getDouble(2));
            }
        } catch (SQLException e){
            System.out.println("something went wrong, task two");
        }

    }

    public void thirdTask(int start, int end){
        String query = String.format(taskThirdQuery, start, end);
        try {
            ResultSet result = this.statement.executeQuery(query);
            while(result.next()){
                System.out.printf("%s: between %d and %d \n",
                        result.getString(1),start,end);
            }
        } catch (SQLException e){
            System.out.println("something went wrong, task two");
        }

    }


    public void stop(){
        try{
            this.statement.close();
            this.con.close();
            System.out.println("worker stoped");
        } catch (SQLException e){
            System.out.println("Something went wrong");
        }
    }

}
