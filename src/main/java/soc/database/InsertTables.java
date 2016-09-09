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
		 String cqlStatement = "CREATE KEYSPACE team3 WITH " + 
                     "replication = {'class':'SimpleStrategy','replication_factor':1}";        
		 session.execute(cqlStatement);
		
		 //create the warehouse
		 cqlStatement = "CREATE TABLE team3.warehouse("+
				 "W_ID	Int, " +
				 "W_NAME Text, "+	
				 "W_STREET_1 Text, "+	
				 "W_STREET_2 Text, "+	
				 "W_CITY Text, " +
				 "W_STATE Text, " +	
				 "W_ZIP	Text, " +	
				 "W_TAX	Decimal, " +	
				 "W_YTD	Decimal, " +
				 "Primary key(W_ID) " +
                 ");";
		 session.execute(cqlStatement);
		 
		 //create the district
		 cqlStatement = "CREATE TABLE team3.district("+
				 "W_ID Int, "+
				 "D_ID int, "+
				 "D_NAME Text, "+	
				 "D_ADDRESS_1 Text, "+	
				 "D_ADDRESS_2 Text,	"+
				 "D_CITY Text, "+
				 "D_STATE Text,	"+
				 "D_ZIP	Text, "+
				 "D_TAX	Decimal, "+	
				 "D_YTD	Decimal, "+
				 "D_NEXT_O_ID Counter, "+	
				 "PRIMARY KEY (D_ID,W_ID)" +
                 ");";
		 session.execute(cqlStatement);
		 
		 
		 cqlStatement = "CREATE TABLE team3.customer("+
				 "W_ID Int, " +	
				 "D_ID Int, " +	
				 "C_ID	Int, "+	
				 "C_FIRST Text, " +	
				 "C_MIDDLE	Text, " +	
				 "C_LAST	Text, " +	
				 "C_STREET_1	Text, " +		
				 "C_STREET_2	Text, " +	
				 "C_CITY	Text, " +	
				 "C_STATE	Text, " +	
				 "C_ZIP	Text, " +	
				 "C_PHONE	Text, " +	
				 "C_SCIENCE	Timestamp, " +	
				 "C_CREDIT_LIM	Decimal, " +	
				 "C_DISCOUNT	Decimal, " +	
				 "C_BALANCE	Decimal, " +	
				 "C_YTD_PAYMENT	Decimal, " +	
				 "C_PAYMENT_CNT	Counter, " +	
				 "C_DELIVERY_CNT	Counter, " +	
				 "C_DATA	Text, " +	
				 "Primary key( (W_ID,D_ID),C_ID) WITH CLUSTERING ORDER BY (C_BALANCE DESC)"+
				 ");";
		 session.execute(cqlStatement);
		 
		 
				 
		 //create item table 
		 cqlStatement = "CREATE TABLE team3.item("+
				 "I_ID INT, "+
				 "I_NAME text, "+
				 "I_PRICE Decimal, "+
				 "I_IM ID INT, "+
				 "I_DATA Text,	"+
				 "PRIMARY KEY (W_ID, D_ID)" +
                 ");";
		 session.execute(cqlStatement);
		 
		 
		 
		 
//		 cqlStatement = "CREATE TABLE team3.stock (" + 
//				 "W _ID	Int, " +
//				 "I_ID	Int, " +
//				 "S_QUANTITY Counter, " +
//				 "S_YTD	Counter," +
//				 "S_ORDER_CNT Counter," +
//				 "S_REMOTE_CNT	Counter," +
//				 "S_DATA Text," +
//				 "PRIMARY KEY ((W_ID, I_ID))" +
//                 ");";
//		 session.execute(cqlStatement);

		 
		 
		 
		 
		 
		 System.out.println("Done");
		 System.exit(0);
	}
	
	public void CreateWareHouse(){
		
	}
}
