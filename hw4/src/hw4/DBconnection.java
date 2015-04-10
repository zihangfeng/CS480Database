package hw4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;
import java.lang.ClassNotFoundException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import com.mysql.jdbc.Statement;
import java.util.StringTokenizer;

public class DBconnection {


		//static reference to itself
		private static DBconnection instance = new DBconnection();
		public static final String URL = "jdbc:mysql://localhost:3306/";
		public static final String USER = "root";
		public static final String PASSWORD = "feng7631321";
		public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver"; 

		//private constructor
		private DBconnection() {
			try {
				//Step 2: Load MySQL Java driver
				Class.forName(DRIVER_CLASS);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		private Connection createConnection() {

			Connection connection = null;
			try {
				//Step 3: Establish Java MySQL connection
				connection = DriverManager.getConnection(URL, USER, PASSWORD);
				System.out.println("Connected");
			} catch (SQLException e) {
				System.out.println("ERROR: Unable to Connect to Database.");
			}
			return connection;
		}	

		public static Connection getConnection() {
			return instance.createConnection();
		}



	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection DB = null;
		Statement statement = null;
		Scanner filein = null;
		ResultSet result = null;
	    try{
	
	    	// start the connection
		    DB = DBconnection.getConnection();
		    statement = (Statement) DB.createStatement(); 
		    
		    // create database ..
		    statement.executeUpdate("CREATE DATABASE EMDB");
		    statement.executeUpdate("use EMDB");
		    
		    // create the tables
		    statement.executeUpdate("CREATE TABLE employee "
					+ "(eid INT(20) NOT NULL,"
					+ "name VARCHAR(20) NOT NULL,"
					+ "salary INT(20) NOT NULL,"
					+ "primary key(eid))");
		    statement.executeUpdate("CREATE TABLE worksfor"
					+ "(eid INT(20) NOT NULL, "
					+ "mid INT(20))");
		    
		    // read the file and get the information
		    filein = new Scanner (new FileInputStream("transfile.txt"));
		    int transaction;
		    int eid, salary, mid;
		    String name;
		    StringTokenizer tokens = null;
		    while(filein.hasNextLine())
		    {
		    	tokens = new StringTokenizer(filein.nextLine());
		    	if(tokens.hasMoreTokens())
		    	{
		    		transaction = Integer.parseInt(tokens.nextToken());
		    		if(transaction == 1)
		    		{
		    			// do the deletion
		    			eid = Integer.parseInt(tokens.nextToken());
		    			
		    			result = statement.executeQuery("select eid from employee where eid = " + eid);
		    			if(result.next())
		    			{
		    				statement.execute("delete from employee " 
		    									+ "where eid = " + eid);
		    				statement.execute("delete from worksfor " 
		    									+ "where eid = " + eid);
		    				System.out.println("Done deleting the employee: " + eid);
		    			}
		    			else
		    			{
		    				System.out.println("Can't delete, no such employee: " + eid + " exists.");
		    			}
		    			
		    			System.out.println("");
		    		}
		    		else if(transaction == 2)
		    		{
		    			// add a new employee to the table and its corresponding
		    			eid = Integer.parseInt(tokens.nextToken());
		    			
		    			name = tokens.nextToken();
		    			salary = Integer.parseInt(tokens.nextToken());
		    			
		    			result = statement.executeQuery("select eid from employee where eid = " + eid);
		    			if(!result.next()){
		    				statement.execute("insert into employee"
		    					+ "(eid,name,salary)"
		    					+ " values(" + eid +"," + "\"" + name + "\"" + "," + salary + ")");
		    			
		    				while(tokens.hasMoreTokens())
		    				{
		    					mid = Integer.parseInt(tokens.nextToken());
		    					statement.execute("insert into worksfor"
		    										+ "(eid, mid)"
		    										+ " values(" + eid +"," + mid + ")");
		    				}
		    				System.out.println("Done adding a new employee: " + eid);
		    			}
		    			else
		    			{
		    				System.out.println("Can't add, employee id: " + eid + " exists");
		    			}
		    		}
		    		else if(transaction == 3)
		    		{
		    			// output the average of salary of all the employees
		    			double Davg = 0;
		    			int avg = 0;
		    			result = statement.executeQuery("select avg(salary) from employee");
		    			if(result.next()){
		    			Davg = result.getDouble(1) + 0.5;
		    			avg = (int)Davg;
		    			System.out.println("The average of all employee is: " + avg);
		    			}
		    			else
		    			{
		    				System.out.println("Can't output the average of all employees.");
		    			}
		    			System.out.println("");
		    		}
		    		else if(transaction == 4)
		    		{
		    			// output the names of all employee that work under a manager directly or indirectly
		    			mid = Integer.parseInt(tokens.nextToken());
		    			String EmName = null;
		    			result = statement.executeQuery("select name from employee natural join worksfor where mid = " + mid);
		    			if(result.next())
		    			{
		    				System.out.println("Employees work under manager: " + mid + " are:");
		    				EmName = result.getString("name");
	    					System.out.println(EmName);
		    				while(result.next())
		    				{
		    					EmName = result.getString("name");
		    					System.out.println(EmName);
		    				}
		    			}
		    			else
		    			{
		    				System.out.println("Can't output the employee names who work under manager: " + mid);
		    			}
		    			System.out.println("");
		    		}
		    		else if (transaction == 5)
		    		{
		    			// output the average salary of employees that work under a manager
		    			mid = Integer.parseInt(tokens.nextToken());
		    			double Davg = 0;
		    			int avg = 0;
		    			result = statement.executeQuery("select avg(salary) from employee natural join worksfor where mid = " + mid);
		    			if(result.next())
		    			{
		    				Davg = result.getDouble(1);
		    				avg = (int)Davg;
		    				System.out.println("The average of the employee works under the manager: " + mid + " is: " + avg);
		    			}
		    			else
		    			{
		    				System.out.println("Can't output the employee average salary who work under manager: " + mid);
		    			}
		    			System.out.println("");
		    		}
		    		else if(transaction == 6)
		    		{
		    			// check if any employee who has more than one manager
		    			String EmName = null;
		    			result = statement.executeQuery("select name from employee natural join worksfor group by eid having count(eid) > 1");
		    			if(result.next())
		    			{
		    				EmName = result.getString("name");
		    				System.out.println("The employees who have more than one manager are:");
		    				System.out.println(EmName);
		    					while(result.next())
		    					{
		    						EmName = result.getString("name");
				    				System.out.println(EmName);
		    					}
		    			}
		    			else
		    			{
		    				System.out.println("There is no such person who has more than one manager");
		    			}
		    			System.out.println("");
		    		}
		    		else
		    		{
		    			// wrong transaction code
		    			System.out.println("At some point in the program you do not have the valid transaction code.");
		    		}
		    	}		
		    }
		    
		    
		    // to close everything else
		    filein.close();
		    statement.executeUpdate("drop table employee");
		    statement.executeUpdate("drop table worksfor");
		    statement.executeUpdate("drop database EMDB");
		    statement.executeUpdate("FLUSH PRIVILEGES");
	    } catch(SQLException se) {
	    	se.printStackTrace();
	    } catch (FileNotFoundException e)
	    {
	    	System.out.println("File not Found");
	    	System.exit(0);
	    }
	    
	    finally
	    {
	    	if(DB != null)	try{DB.close();}
	    		catch(SQLException ignore){}
	    	if(statement != null)	try{statement.close();}
    			catch(SQLException ignore){}
	    	if(result != null)	try{result.close();}
    			catch(SQLException ignore){}
	    }
	}
}
