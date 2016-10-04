package database;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class ImportDataModified {
	static String path = "./data/D8-data";
	static String nodeIP = "127.0.0.1";
	static String keyspace = "team3";
	 //192.168.48.227    xcnd8.comp.nus.edu.sg
//	insert.importItem();
	public static void main(String[] args) {
		ImportDataModified insert = new ImportDataModified();
//		insert.importWareHouse();
//		insert.importDistrict();
//		insert.importCustomer();
//		insert.importOrder();

//		insert.importOrderLine();
//		insert.importStock();    //import stock table first, then update StockItem table
		insert.updateItem();
		
	}
	
	public void importWareHouse(){
		
		String fileName = path + "/warehouse.csv";

		String line = null;
		String statement;
		
		Cluster cluster;
		Session session;
		
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
	
				statement = "Insert into team3.warehouse (" +
						 "W_ID	, " +
						 "W_NAME , "+	
						 "W_STREET_1 , "+	
						 "W_STREET_2 , "+	
						 "W_CITY , " +
						 "W_STATE , " +	
						 "W_ZIP	, " +	
						 "W_TAX	, " +	
						 "W_YTD	)" + " values("+
						 "" + content[0]+","+
						 "'" + content[1]+"',"+
						 "'" + content[2]+"',"+
						 "'" + content[3]+"',"+
						 "'" + content[4]+"',"+
						 "'" + content[5]+"',"+
						 "'" + content[6]+"',"+
						 "" + content[7]+","+ 
						 "" + content[8]+ ") IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("W_ID " +content[0]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}

		cluster.close();
		System.out.println("Warehouse Inserted.");
	}

	public void importDistrict() {
		String fileName = path + "/district.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
	
				statement = "Insert into team3.District (" +
						 	"D_W_ID ,"+
							"D_ID ,"+
							"D_NAME ,"+
							"D_STREET_1 ,"+
							"D_STREET_2 ,"+
							"D_CITY ,"+
							"D_STATE ,"+
							"D_ZIP ,"+
							"D_TAX ,"+
							"D_YTD ,"+
							"D_NEXT_O_ID )" + " values("+
							"" + content[0]+","+
							"" + content[1]+","+
							"'" + content[2]+"',"+
							"'" + content[3]+"',"+
							"'" + content[4]+"',"+
							"'" + content[5]+"',"+
							"'" + content[6]+"',"+
							"'" + content[7]+"',"+ 
							"" + content[8]+","+
							"" + content[9]+","+ 
							"" + content[10]+ ")IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("D_W_ID " +content[0] + " D_ID " + content[1]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("District Inserted.");
	}
	
	public void importCustomer(){
		String fileName = path + "/customer.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

	/*	
		// Prepare SSTable writer 
		CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
		// set output directory 
		builder.inDirectory(outputDir)
		       // set target schema 
		       .forTable(SCHEMA)
		       // set CQL statement to put data 
		       .using(INSERT_STMT)
		       // set partitioner if needed 
		       // default is Murmur3Partitioner so set if you use different one. 
		       .withPartitioner(new Murmur3Partitioner());
		CQLSSTableWriter writer = builder.build();
		 
		// ...snip... 
		 
		while ((line = csvReader.read()) != null)
		{
		    // We use Java types here based on 
		    // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29 
		    writer.addRow(ticker,
		                  DATE_FORMAT.parse(line.get(0)),
		                  new BigDecimal(line.get(1)),
		                  new BigDecimal(line.get(2)),
		                  new BigDecimal(line.get(3)),
		                  new BigDecimal(line.get(4)),
		                  Long.parseLong(line.get(5)),
		                  new BigDecimal(line.get(6)));
		}
		writer.close();
		
		
		*/
		
		
		
		
		
		
		
		
		
		
		
		
		
		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
	
				statement = "Insert into team3.Customer (" +
						"C_W_ID ,"+
						"C_D_ID ,"+
						"C_ID ,"+
						"C_FIRST ,"+
						"C_MIDDLE ,"+
						"C_LAST ,"+
						"C_STREET_1 ,"+
						"C_STREET_2 ,"+
						"C_CITY ,"+
						"C_STATE ,"+
						"C_ZIP ,"+
						"C_PHONE ,"+
						"C_SINCE ,"+
						"C_CREDIT ,"+
						"C_CREDIT_LIM ,"+
						"C_DISCOUNT ,"+
						"C_BALANCE ,"+
						"C_YTD_PAYMENT ,"+
						"C_PAYMENT_CNT ,"+
						"C_DELIVERY_CNT ,"+
						"C_DATA  )" + " values("+
						"" + content[0]+","+
						"" + content[1]+","+
						"" + content[2]+","+
						"'" + content[3]+"',"+
						"'" + content[4]+"',"+
						"'" + content[5]+"',"+
						"'" + content[6]+"',"+
						"'" + content[7]+"',"+
						"'" + content[8]+"',"+ 
						"'" + content[9]+"',"+
						"'" + content[10]+"',"+
						"'" + content[11]+"',"+
//							"toTimestamp(" + content[12]+"),"+   //timestamp
						"blobAsBigint(timestampAsBlob('" + content[12]+"')),"+ 
						"'" + content[13]+"',"+
						"" + content[14]+","+
						"" + content[15]+","+
						"" + content[16]+","+ 
						"" + content[17]+","+
						"" + content[18]+","+
						"" + content[19]+","+ 
						"'" + content[20]+ "')IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("C_W_ID " +content[0] + " C_D_ID " + content[1]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("Customer Inserted.");
		
	}
	
	public void importOrder(){
		String fileName = path + "/order.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
	
				if(content[4].endsWith("null")){ content[4]="-1";}
				statement = "Insert into team3.Orders (" +
							"O_W_ID ,"+ 
							"O_D_ID ,"+ 
							"O_ID ,"+ 
							"O_C_ID ,"+ 
							"O_CARRIER_ID ,"+ 
							"O_OL_CNT ,"+ 
							"O_ALL_LOCAL ,"+ 
							"O_ENTRY_D )" + " values("+
							"" + content[0]+","+
							"" + content[1]+","+
							"" + content[2]+","+
							"" + content[3]+","+
							"" + content[4]+","+
							"" + content[5]+","+
							"" + content[6]+","+
							"blobAsBigint(timestampAsBlob('" + content[7]+"'))"+
							")IF NOT EXISTS;";
				session.execute(statement);
				System.out.println("O_W_ID " +content[0] + " O_D_ID " + content[1]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("Orders Inserted.");
	}
	
/*
	public void importItem(){
		String fileName = path + "/item.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
				
			
				statement = "Insert into team3.Item (" +
							"I_ID ,"+ 
							"I_NAME ,"+ 
							"I_PRICE ,"+ 
							"I_IM_ID ,"+ 
							"I_DATA )" + " values("+
							"" + content[0]+","+
							"'" + content[1]+"',"+
							"" + content[2]+","+
							"" + content[3]+","+
							"'" + content[4]+ "')IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("I_ID " +content[0] + " I_PRICE " + content[2]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("Item Inserted.");
		
	}
	*/
	
	public void importOrderLine(){
		String fileName = path + "/order-line.csv";
		
		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
				if(content[5].endsWith("null")) content[5]="";
				statement = "Insert into team3.OrderLine (" +
							"OL_W_ID ,"+ 
							"OL_D_ID ,"+ 
							"OL_O_ID ,"+ 
							"OL_NUMBER ,"+ 
							"OL_I_ID ,"+ 
							"OL_DELIVERY_D ,"+ 
							"OL_AMOUNT ,"+ 
							"OL_SUPPLY_W_ID ,"+ 
							"OL_QUANTITY ,"+ 
							"OL_DIST_INFO )" + " values("+
							"" + content[0]+","+
							"" + content[1]+","+
							"" + content[2]+","+
							"" + content[3]+","+
							"" + content[4]+","+
							"blobAsBigint(timestampAsBlob('" + content[5]+"')),"+ 
							
							"" + content[6]+","+
							"" + content[7]+","+
							"" + content[8]+","+	
							"'" + content[9]+"')IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("OL_I_ID " +content[4]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("OrderLine Inserted.");
	}
	
	public void importStock(){
		String fileName = path + "/stock.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
				
			
				statement = "Insert into team3.StockItem (" +
							"S_W_ID ,"+ 
							"S_I_ID ,"+ 
							"S_QUANTITY ,"+ 
							"S_YTD ,"+ 
							"S_ORDER_CNT ,"+ 
							"S_REMOTE_CNT ,"+ 
							"S_DIST_01 ,"+ 
							"S_DIST_02 ,"+ 
							"S_DIST_03 ,"+ 
							"S_DIST_04 ,"+ 
							"S_DIST_05 ,"+ 
							"S_DIST_06 ,"+ 
							"S_DIST_07 ,"+ 
							"S_DIST_08 ,"+ 
							"S_DIST_09 ,"+ 
							"S_DIST_10 ,"+ 
							"S_DATA )" + " values("+
							"" + content[0]+","+
							"" + content[1]+","+
							"" + content[2]+","+
							"" + content[3]+","+
							"" + content[4]+","+
							"" + content[5]+","+
							
							"'" + content[6]+"',"+
							"'" + content[7]+"',"+
							"'" + content[8]+"',"+
							"'" + content[9]+"',"+
							"'" + content[10]+"',"+
							"'" + content[11]+"',"+
							"'" + content[12]+"',"+
							"'" + content[13]+"',"+
							"'" + content[14]+"',"+
							"'" + content[15]+"',"+
							"'" + content[16]+ "')IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("S_W_ID " +content[0] + " S_QUANTITY " + content[2]);		 
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("Stock Inserted.");
	}
	
	
	public void updateItem(){
		String fileName = path + "/item.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
				
				statement = "Update team3.StockItem SET "+
							"I_NAME = '" + content[1]+ "', "+
							"I_PRICE = " + content[2]+ ", "+
							"I_IM_ID = " + content[3]+ ", "+
							"I_DATA = '" + content[4]+ "' "+
							"where S_W_ID in (1,2,3,4,5,6,7,8) AND S_I_ID = " + content[0]+ "; ";
						
//							"'" + content[4]+ "')IF NOT EXISTS;";		
				session.execute(statement);
				System.out.println("Item. " +content[0]);
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("Item Updated.");
		
	}
	
	
//reference
//	http://stackoverflow.com/questions/28547616/cassandra-cqlsh-how-to-show-microseconds-milliseconds-for-timestamp-columns	
}


