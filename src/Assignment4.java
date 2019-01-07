import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.io.File;

import javafx.util.Pair;

import java.util.ArrayList;



public class Assignment4 {
    private final String databaseName = "[DB2019_Ass2]";
    private final DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);


    private Assignment4() {
    }

   public static void executeFunc(Assignment4 ass, String[] args) {
        String funcName = args[0];
        switch (funcName) {
            case "loadNeighborhoodsFromCsv":
                ass.loadNeighborhoodsFromCsv(args[1]);
                break;
            case "dropDB":
                ass.dropDB();
                break;
            case "initDB":
                ass.initDB(args[1]);
                break;
            case "updateEmployeeSalaries":
                ass.updateEmployeeSalaries(Double.parseDouble(args[1]));
                break;
            case "getEmployeeTotalSalary":
                System.out.println(ass.getEmployeeTotalSalary());
                break;
            case "updateAllProjectsBudget":
                ass.updateAllProjectsBudget(Double.parseDouble(args[1]));
                break;
            case "getTotalProjectBudget":
                System.out.println(ass.getTotalProjectBudget());
                break;
            case "calculateIncomeFromParking":
                System.out.println(ass.calculateIncomeFromParking(Integer.parseInt(args[1])));
                break;
            case "getMostProfitableParkingAreas":
                System.out.println(ass.getMostProfitableParkingAreas());
                break;
            case "getNumberOfParkingByArea":
                System.out.println(ass.getNumberOfParkingByArea());
                break;
            case "getNumberOfDistinctCarsByArea":
                System.out.println(ass.getNumberOfDistinctCarsByArea());
                break;
            case "AddEmployee":
                SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
                ass.AddEmployee(Integer.parseInt(args[1]), args[2], args[3], java.sql.Date.valueOf(args[4]), args[5], Integer.parseInt(args[6]), Integer.parseInt(args[7]), args[8]);
                break;
            default:
                break;
        }
    }



    public static void main(String[] args) {

        File file = new File(".");
        String csvFile = "src/filename.csv";
        String line = "";
        String cvsSplitBy = ",";
        Assignment4 ass = new Assignment4();

        ass.updateAllProjectsBudget(100);
        System.out.println(ass.getEmployeeTotalSalary());
        System.out.println(ass.getMostProfitableParkingAreas());
        System.out.println(ass.getNumberOfParkingByArea());
        System.out.println(ass.getNumberOfDistinctCarsByArea());

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] row = line.split(cvsSplitBy);

                executeFunc(ass, row);

            }

        } catch (IOException e) {
            e.printStackTrace();

        }
    }



    private void loadNeighborhoodsFromCsv(String csvPath) {
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new FileReader(csvPath));
            while ((line = br.readLine()) != null) {
                String[] row = line.split(cvsSplitBy);
                DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
                databaseManager.startConnection();
                int id = Integer.parseInt(row[0]);
                String name = "'" + row[1] + "'";
                databaseManager.executeQueryVoid("INSERT INTO Neighborhood VALUES(" + id + "," + name + ")");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        databaseManager.closeConnection();
    }

    private void updateEmployeeSalaries(double percentage) {
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        databaseManager.executeQueryVoid("UPDATE ConstructorEmployee\n" +
                "SET SalaryPerDay = SalaryPerDay * " + (1 + (percentage/100.0)) + "\n" +
                "WHERE EID IN (SELECT ConstructorEmployee.EID\n" +
                "\tFROM Employee, ConstructorEmployee\n" +
                "\tWHERE Employee.EID = ConstructorEmployee.EID AND DATEDIFF(YEAR, Employee.BirthDate, GETDATE()) >= 50)");
        databaseManager.closeConnection();
    }


    public void updateAllProjectsBudget(double percentage) {
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        databaseManager.executeQueryVoid("UPDATE Project\n" +
            "SET Budget = Budget * " + (1 + (percentage/100.0)) + "\n");
        databaseManager.closeConnection();
    }


    private double getEmployeeTotalSalary() {
        double ret = 0;
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        ResultSet result = databaseManager.executeQuerySelect("SELECT SUM(SalaryPerDay) as Total\n" +
                "FROM ConstructorEmployee");
        try {
            result.next();
            ret = result.getDouble(1);
            result.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        databaseManager.closeConnection();
        return ret;
    }


    private int getTotalProjectBudget() {
        int ret = 0;
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        ResultSet result = databaseManager.executeQuerySelect("SELECT SUM(Budget)\n" +
                "FROM Project");
        try {
            result.next();
            ret = result.getInt(1);
            result.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        databaseManager.closeConnection();
        return ret;

    }
    private void dropDB() {
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();

        String query = "DROP DATABASE DB2019_Ass2";

        databaseManager.executeQueryVoid(query);
        databaseManager.closeConnection();
    }

    private void initDB(String csvPath) {
        DatabaseManager master = new DatabaseManagerMSSQLServer(databaseName);
        String line = "";
        String cvsSplitBy = ",";
        String file = "";
        String query="";
        master.startConnection();
        //sql file
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            while ((line = br.readLine()) != null) {
                //not a comment
                if(!line.contains("--"))
                    file = file + line + " ";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(file != ""){
            //create database

            String [] createDatabase = file.split("GO");
            for(String queries : createDatabase){
                master.executeQueryVoid(queries);
            }
        }
        master.closeConnection();
    }
    private int calculateIncomeFromParking(int year) {
        int ret = 0;
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        ResultSet result = databaseManager.executeQuerySelect("SELECT SUM(Cost)\n" +
                "FROM CarParking\n"+
                "WHERE YEAR(StartTime) = " + year);
        try {
            result.next();
            ret = result.getInt(1);
            result.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        databaseManager.closeConnection();
        return ret;
    }

    private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
        ArrayList<Pair<Integer, Integer>> ret = new ArrayList<>();
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        ResultSet result = databaseManager.executeQuerySelect("SELECT TOP 5 AID, SUM(CarParking.Cost as TotalCost\n" +
                "FROM ParkingArea LEFT JOIN CarParking ON ParkingArea.[AID] = CarParking.[ParkingAreaID]\n" +
                "GROUP BY AID\n" +
                "ORDER BY TotalCost");

        try {
            while (result.next())
                ret.add(new Pair<>(result.getInt(1), result.getInt(2)));
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return ret;
    }

    private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {
        ArrayList<Pair<Integer, Integer>> ret = new ArrayList<>();
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        ResultSet result = databaseManager.executeQuerySelect("SELECT AID, COUNT(CarParking.AID) as Total\n" +
                "FROM ParkingArea LEFT JOIN CarParking ON ParkingArea.[AID] = CarParking.[ParkingAreaID]\n" +
                "GROUP BY AID");

        try {
            while (result.next())
                ret.add(new Pair<>(result.getInt(1), result.getInt(2)));
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return ret;
    }


    private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {

        ArrayList<Pair<Integer, Integer>> ret = new ArrayList<>();
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        ResultSet result = databaseManager.executeQuerySelect("SELECT AID, COUNT(DISTINCT CarParking.CID) as Total\n" +
                "FROM ParkingArea LEFT JOIN CarParking ON ParkingArea.[AID] = CarParking.[ParkingAreaID]\n" +
                "GROUP BY AID");

        try {
            while (result.next())
                ret.add(new Pair<>(result.getInt(1), result.getInt(2)));
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return ret;
    }


    private void AddEmployee(int EID, String LastName, String FirstName, Date BirthDate, String StreetName, int Number, int door, String City) {
        DatabaseManager databaseManager = new DatabaseManagerMSSQLServer(databaseName);
        databaseManager.startConnection();
        databaseManager.executeQueryVoid("INSERT INTO Employee(EID, LastName, FirstName, BirthDate, StreetName, Number, door, City) VALUES (\n" +
                EID + ", '" +
                LastName + "', '" +
                FirstName + "', '" +
                BirthDate + "', '" +
                StreetName + "', " +
                Number + ", " +
                door + ", " +
                City + ")");
        databaseManager.closeConnection();
    }
}
