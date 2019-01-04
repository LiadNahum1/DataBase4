import java.io.*;
import java.sql.*;
import java.util.Calendar;
import java.text.SimpleDateFormat;

import javafx.util.Pair;

import java.util.ArrayList;



public class Assignment4 {
    private DatabaseManager dM;
    private DatabaseManager master;
    private String databaseName = "DB2019_Ass2";
    private Assignment4() {
        this.dM = new DatabaseManagerMSSQLServer(databaseName);
        this.master = new DatabaseManagerMSSQLServer("dbmaster");
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
                //args 4 in the format of yyyy-mm-dd
                SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
                ass.AddEmployee(Integer.parseInt(args[1]), args[2], args[3], java.sql.Date.valueOf(args[4]), args[5], Integer.parseInt(args[6]), Integer.parseInt(args[7]), args[8]);
                break;
            default:
                break;
        }
    }



    public static void main(String[] args) {
        File file = new File(".");
        String csvFile = args[0];
        String line = "";
        String cvsSplitBy = ",";
        Assignment4 ass = new Assignment4();
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
        String line = "";
        String cvsSplitBy = ",";
        dM.startConnection();
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] row = line.split(cvsSplitBy);
                dM.executeQuery("INSERT INTO Neighborhood VALUES(" + row[0]+ ",'" + row[1]+"');");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            dM.closeConnection();
        }
    }

    private void updateEmployeeSalaries(double percentage) {
        dM.startConnection();
        String query = "UPDATE [ConstructorEmployee] SET SalaryPerDay= SalaryPerDay*(100+" +percentage +")/100 FROM [Employee] JOIN [ConstructorEmployee] ON Employee.EID=ConstructorEmployee.EID" +
                " WHERE DATEDIFF(YEAR,Employee.BirthDate, GETDATE()) >= 50;";
        dM.executeQuery(query);
        dM.closeConnection();
    }


    public void updateAllProjectsBudget(double percentage) {
        dM.startConnection();
        String query = "UPDATE [Project] SET Budget = Budget*(100+" +percentage+ ")/100;";
        dM.executeQuery(query);
        dM.closeConnection();
    }


    private double getEmployeeTotalSalary() {
        int totalSal=0;
        dM.startConnection();
        String query ="SELECT SUM(SalaryPerDay) AS TotalSal FROM ConstructorEmployee;";
        ResultSet rs= dM.executeQuerySelect(query);
        try {
            if(rs.next()) {
                totalSal = rs.getInt("TotalSal");
            }
        }
        catch(Exception e){
            //TODO EARASE
            System.out.println("Exception");
        }
        dM.closeConnection();
        return totalSal;
    }

    private int getTotalProjectBudget() {
        int totalBudg=0;
        dM.startConnection();
        String query ="SELECT SUM(Budget) AS Totalbudg FROM Project;";
        ResultSet rs= dM.executeQuerySelect(query);
        try {
            if(rs.next()) {
                totalBudg= rs.getInt("TotalBudg");
            }
        }
        catch(Exception e){//TODO EARASE
            System.out.println("Exception");
        }
        dM.closeConnection();
        return totalBudg;
    }
    private void dropDB() {
        master.startConnection();
        String query = "DROP DATABASE " + databaseName+";";
        master.executeQuery(query);
        master.closeConnection();
    }

    private void initDB(String csvPath) {
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
                    file = file + line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(file != ""){
            //create database
            String [] createDatabase = file.split("GO");
            query = createDatabase[0];
            master.executeQuery(query);
            query = createDatabase[1];
            master.executeQuery(query);
            //create tables
            file = createDatabase[2];
            int indexOfQuery = file.indexOf("CREATE");
            int indexOfEnd = file.indexOf(";");
            while(indexOfQuery != -1 & indexOfEnd != -1){
                query = file.substring(indexOfQuery, indexOfEnd);
                master.executeQuery(query);
                file = file.substring(indexOfEnd +1) + ";";
                indexOfQuery = file.indexOf("CREATE");
                indexOfEnd = file.indexOf(";");
            }


        }
        master.closeConnection();
    }
    private int calculateIncomeFromParking(int year) {
        dM.startConnection();
        String query = "SELECT Sum(Cost) as sumC from CarParking WHERE YEAR(ENDTIME)='" +year +"';";
        ResultSet st = dM.executeQuerySelect(query);
        int re = 0;
        try{
            if(st.next()) {
                re =st.getInt("sumC");
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        dM.closeConnection();
        return re;
    }

    private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
        dM.startConnection();
        String query = "SELECT Top 5 ParkingAreaID, SUM(Cost) as price from [CarParking] GROUP BY (ParkingAreaID) ORDER BY price DESC";
        ResultSet rs = dM.executeQuerySelect(query);
        ArrayList<Pair<Integer, Integer>> al = new ArrayList<>();
        try {
            while (rs.next()) {
                Pair p = new Pair(rs.getInt("ParkingAreaID"), rs.getInt("price"));
                al.add(p);
            }
        }
        catch (Exception e){
            return null;
        }
        dM.closeConnection();
        return al;
    }

    private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {
        ArrayList<Pair<Integer,Integer>> numberOfParkingByArea = new ArrayList<>();
        dM.startConnection();
        String query ="SELECT ParkingAreaID, COUNT(*) AS numParking FROM CarParking GROUP BY ParkingAreaID ";
        ResultSet rs= dM.executeQuerySelect(query);
        try {
            while (rs.next()) {
                numberOfParkingByArea.add(new Pair(new Integer(rs.getInt("ParkingAreaID")), new Integer(rs.getInt("numParking"))));
            }
        }
        catch(Exception e){//TODO EARASE
            System.out.println("Exception");
        }
        dM.closeConnection();
        return numberOfParkingByArea;
    }


    private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {
        ArrayList<Pair<Integer,Integer>> numberOfDistinctCars = new ArrayList<>();
        dM.startConnection();
        String query ="SELECT ParkingAreaID, COUNT(DISTINCT CID) AS numCID FROM CarParking GROUP BY ParkingAreaID";
        ResultSet rs= dM.executeQuerySelect(query);
        try {
            while (rs.next()) {
                numberOfDistinctCars.add(new Pair(new Integer(rs.getInt("ParkingAreaID")), new Integer(rs.getInt("numCID"))));
            }
        }
        catch(Exception e){//TODO EARASE
            System.out.println("Exception");
        }
        dM.closeConnection();
        return numberOfDistinctCars;
    }


    private void AddEmployee(int EID, String LastName, String FirstName, Date BirthDate, String StreetName, int Number, int door, String City) {
        dM.startConnection();
        String query = "INSERT INTO Employee(EID,LastName,FirstName,BirthDate,StreetName,Number,door,City) VALUES" +
                " (" +EID+ ",'" +LastName+ "','" + FirstName+"','" +BirthDate+"','" +StreetName+"'," +Number+"," +door+",'" +City+ "');";
        dM.executeQuery(query);
        dM.closeConnection();
    }
}
