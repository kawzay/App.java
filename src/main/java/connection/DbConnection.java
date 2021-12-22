package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DbConnection {

    private static DbConnection dbConnection;
    private static Connection con ;
    private static Statement stmt;


    private DbConnection() {
        // private constructor //
    }

    public static DbConnection getInstance(){
        if(dbConnection == null){
            dbConnection = new DbConnection();
        }
        return dbConnection;
    }

    public Connection getConnection(){

        if(con==null){
            try {
                String host = "jdbc:sqlite:src/main/resources/school.db";
                Class.forName("org.sqlite.JDBC");
                con = DriverManager.getConnection( host );
            } catch (SQLException | ClassNotFoundException ex) {
                ex.printStackTrace();
            }
        }

        return con;
    }
}
