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

public class Denormalize {
	private static String FILEPATH = "./data/D%1$s-data";
	private static String NODEIP = "127.0.0.1";
	private String path;
	
	public Denormalize(String dbType) {
		this.path = String.format(FILEPATH, dbType);
	}
	
	private void importStock(){
		String fileName = path + "/stock.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(NODEIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

            System.out.print("Loading stocks data...");
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
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("success");
	}
	
	private void updateItem(){
		String fileName = path + "/item.csv";

		Cluster cluster;
		Session session;
		String line = null;
		String statement;
				
		cluster = Cluster.builder().addContactPoint(NODEIP).build();
		session = cluster.connect();

		BufferedReader bufferRead = null;
		try {
			FileReader fileReader = new FileReader(fileName);
			bufferRead = new BufferedReader(fileReader);

            System.out.print("Loading items data...");
			while ((line = bufferRead.readLine()) != null) {
				String[] content = line.split(",");
				
				statement = "Update team3.StockItem SET "+
							"I_NAME = '" + content[1]+ "', "+
							"I_PRICE = " + content[2]+ ", "+
							"I_IM_ID = " + content[3]+ ", "+
							"I_DATA = '" + content[4]+ "' "+
							"where S_W_ID in (1,2,3,4,5,6,7,8) AND S_I_ID = " + content[0]+ "; ";	
				session.execute(statement);
			}
			bufferRead.close();  //must be closed
		} catch (FileNotFoundException ex) {
			System.out.println("Could not find:  '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("reading file Error '" + fileName + "'");
		}
		cluster.close();
		System.out.println("success");
		
	}
	
	public static void main(String[] args) {
		if(args.length == 1) {
			Denormalize denormalize = new Denormalize(args[0]);
			denormalize.importStock();    //import stock table first, then update StockItem table
			denormalize.updateItem();
		}
	}
}