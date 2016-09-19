package soc.database;

import com.datastax.driver.core.*;

public class orderStatus {

	private Session session;
	private String keyspace;
	
	private PreparedStatement orderStatus_O_select;
	private PreparedStatement orderst_C_select;	
	
	public orderStatus(int c_w_id, int c_d_id, int c_id) {
		
		orderStatus_O_select = session.prepare("SELECT c_name, o_id, o_entry_d, o_carrier_id, i_id, ol_supply_w_id, ol_qty, ol_amount, ol_delivery_d FROM "
				+ keyspace + ".orders WHERE o_w_id = ? and o_d_id = ? and c_id = ? LIMIT 1;");
	
		orderst_C_select = session.prepare("SELECT c_balance FROM " 
				+ keyspace + ".customer WHERE c_w_id = ? and c_d_id = ? and c_id = ?;");
		
		
		BoundStatement bound_o_sel = new BoundStatement(orderStatus_O_select);
		ResultSet orderResult = session.execute(bound_o_sel.bind(c_w_id, c_d_id, c_id));

		BoundStatement bound_c_sel = new BoundStatement(orderst_C_select);
		ResultSet customerResult = session.execute(bound_c_sel.bind(c_w_id, c_d_id, c_id));
		String output = "";
		
		for (Row row : orderResult) {
			output = output + "Customer's Name : " + row.getString("c_name");
			for (Row row1 : customerResult) {
				output = output + ", Balance: " + ((double) (row1.getLong("c_balance")))/100;
			}
		
			
			output = output + "Customer last order : " 
			+ ", Order number : " + row.getInt("o_id") 
			+ ", Entry date and time: " + row.getDate("o_entry_d") 
			+ ", Carrier identifier: " + row.getInt("o_carrier_id") 
			+ ", Item number: " + row.getInt("i_id") 
			+ ", Supplying warehouse number : " + row.getInt("ol_supply_w_id") 
			+ ", Quantity ordered: " + row.getInt("ol_qty") 
			+ ", Total price for ordered item: " + row.getDouble("ol_amount") 
			+ ", Data and time of delivery:" + row.getDate("ol_delivery_d");
		
		}
		
		System.out.println(output);
	}
	
}
