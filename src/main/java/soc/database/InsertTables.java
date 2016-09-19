package soc.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;



public class InsertTables {
	private Cluster cluster;
	private static Session session;
	public static void main(String[] args) {
		 Cluster cluster = Cluster.builder()
                 .addContactPoints("127.0.0.1")
                 .build();
		 Session session = cluster.connect();

		 //Session session = bdt.session;
		 
		 //create the keyspace
		 String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS team3 WITH " + 
                     "replication = {'class':'SimpleStrategy','replication_factor':1}";        
		 session.execute(cqlStatement);
		
		 //create the warehouse
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.Warehouse("+
				 	"W_ID int,"+
					"W_NAME varchar,"+
					"W_STREET_1 varchar,"+
					"W_STREET_2 varchar,"+
					"W_CITY varchar,"+
					"W_STATE varchar,"+
					"W_ZIP varchar,"+
					"W_TAX decimal,"+
					"W_YTD decimal,"+
					"PRIMARY KEY (W_ID)"+
                 ");";
		 session.execute(cqlStatement);
		 System.out.println("warehouse created");
		 
		 //create the district
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.District("+
				    "D_W_ID int,"+
					"D_ID int,"+
					"D_NAME varchar,"+
					"D_STREET_1 varchar,"+
					"D_STREET_2 varchar,"+
					"D_CITY varchar,"+
					"D_STATE varchar,"+
					"D_ZIP varchar,"+
					"D_TAX decimal,"+
					"D_YTD decimal,"+
					"D_NEXT_O_ID int,"+
					"PRIMARY KEY ((D_W_ID), D_ID)"+
                 ");";
		 session.execute(cqlStatement);
		 System.out.println("District created");
		 
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.Customer("+
				 	"C_W_ID int,"+
					"C_D_ID int,"+
					"C_ID int,"+
					"C_FIRST varchar,"+
					"C_MIDDLE varchar,"+
					"C_LAST varchar,"+
					"C_STREET_1 varchar,"+
					"C_STREET_2 varchar,"+
					"C_CITY varchar,"+
					"C_STATE varchar,"+
					"C_ZIP varchar,"+
					"C_PHONE varchar,"+
					"C_SINCE timestamp,"+
					"C_CREDIT varchar,"+
					"C_CREDIT_LIM decimal,"+
					"C_DISCOUNT decimal,"+
					"C_BALANCE decimal,"+
					"C_YTD_PAYMENT float,"+
					"C_PAYMENT_CNT int,"+
					"C_DELIVERY_CNT int,"+
					"C_DATA varchar,"+
					"PRIMARY KEY((C_W_ID, C_D_ID), C_ID)"+ 
				 ");";
		 session.execute(cqlStatement);
		 System.out.println("Customer created");
		 
		 //create orders table
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.Orders("+
					"O_W_ID int,"+ 
					"O_D_ID int,"+ 
					"O_ID int,"+ 
					"O_C_ID int,"+ 
					"O_CARRIER_ID int,"+ 
					"O_OL_CNT decimal,"+ 
					"O_ALL_LOCAL decimal,"+ 
					"O_ENTRY_D timestamp,"+ 
					"PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)"+ 
				") WITH CLUSTERING ORDER BY (O_ID DESC)"+ 
                 ";";
		 session.execute(cqlStatement);
		 System.out.println("Orders created");
		 
		 
				 
		 //create item table 
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.Item("+
				 	"I_ID int,"+ 
					"I_NAME varchar,"+ 
					"I_PRICE decimal,"+ 
					"I_IM_ID int,"+ 
					"I_DATA varchar,"+ 
					"PRIMARY KEY (I_ID)"+ 
                 ");";
		 session.execute(cqlStatement);
		 System.out.println("Item created");
		 
		//create item orderLine 
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.OrderLine("+
				 	"OL_W_ID int,"+ 
					"OL_D_ID int,"+ 
					"OL_O_ID int,"+ 
					"OL_NUMBER int,"+ 
					"OL_I_ID int,"+ 
					"OL_DELIVERY_D timestamp,"+ 
					"OL_AMOUNT decimal,"+ 
					"OL_SUPPLY_W_ID int,"+ 
					"OL_QUANTITY decimal,"+ 
					"OL_DIST_INFO varchar,"+ 
					"PRIMARY KEY ((OL_W_ID, OL_D_ID, OL_O_ID), OL_NUMBER)"+ 
                 ");";
		 session.execute(cqlStatement);
		 System.out.println("OrderLine created");
	
		 
		 cqlStatement = "CREATE TABLE IF NOT EXISTS team3.Stock (" + 
				 	"S_W_ID int,"+ 
					"S_I_ID int,"+ 
					"S_QUANTITY decimal,"+ 
					"S_YTD decimal,"+ 
					"S_ORDER_CNT int,"+ 
					"S_REMOTE_CNT int,"+ 
					"S_DIST_01 varchar,"+ 
					"S_DIST_02 varchar,"+ 
					"S_DIST_03 varchar,"+ 
					"S_DIST_04 varchar,"+ 
					"S_DIST_05 varchar,"+ 
					"S_DIST_06 varchar,"+ 
					"S_DIST_07 varchar,"+ 
					"S_DIST_08 varchar,"+ 
					"S_DIST_09 varchar,"+ 
					"S_DIST_10 varchar,"+ 
					"S_DATA varchar,"+ 
					"PRIMARY KEY ((S_W_ID), S_I_ID)"+ 
                 ");";
		 session.execute(cqlStatement);
		 System.out.println("Stock created");
		 
		 
		 System.out.println("Done");
		 System.exit(0);
	}
}