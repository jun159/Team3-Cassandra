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
	
	private void updateItem(){
		String fileName = path + "/item.csv";
		int current = 0;

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
				System.out.print(String.format("Rows imported from item: %1$s\r", current++));
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
			denormalize.updateItem();
		}
	}
}