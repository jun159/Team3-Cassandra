
package app;

import com.datastax.driver.core.*;

public class OrderStatus {

	private Session session;
	private static CassandraConnect connect;
	
	private PreparedStatement customerSelect;
	
	private PreparedStatement orderSelect;
	private PreparedStatement orderStatus_O_select2;
	
	private PreparedStatement orderst_OL_select;	
	
	public OrderStatus(CassandraConnect connect) {
		this.session=connect.getSession();
		customerSelect = session.prepare("SELECT c_first, c_middle, c_last, c_balance FROM "
				 + "Customer WHERE c_w_id = ? and c_d_id = ? and c_id = ?;");
		
		orderSelect = session.prepare("SELECT o_id,o_c_id FROM "
				 + "orders WHERE o_w_id = ? and o_d_id = ?;");
	
		orderStatus_O_select2 = session.prepare("SELECT o_entry_d, o_carrier_id FROM "
				 + "orders WHERE o_w_id = ? and o_d_id = ? and o_id = ?;");
		
		
		orderst_OL_select = session.prepare("SELECT OL_I_ID, OL_SUPPLY_W_ID ,OL_QUANTITY,OL_AMOUNT,"
				+ "OL_DELIVERY_D FROM " 
				 + "OrderLine WHERE ol_w_id = ? and ol_d_id = ? and ol_o_id = ?;");
		
	}
	
	
	public void processOrderStatus(int C_W_ID, int C_D_ID, int C_ID) {
		
		//1. output customer name
		BoundStatement c_select = new BoundStatement(customerSelect);
		ResultSet result = session.execute(c_select.bind(C_W_ID, C_D_ID, C_ID));
//		int c_id =-1;
		for (Row row : result) {
//			c_id = row.getInt("c_id");
			System.out.println("Customer Name: " + row.getString("c_first") +" "+row.getString("c_middle")
					+ " "+ row.getString("c_last")+ " Balance: " + row.getDouble("c_balance"));
		}
		
		//2. output customer last order
		BoundStatement o_select = new BoundStatement(orderSelect);
		result = session.execute(o_select.bind(C_W_ID, C_D_ID));
		
		int largestOrder=-1;
		for (Row row : result) {
			if(row.getInt("o_c_id")==C_ID){
				if(largestOrder<row.getInt("o_id"))
				largestOrder = row.getInt("o_id");
			}
		}
		
		BoundStatement o_select2 = new BoundStatement(orderStatus_O_select2);
		result = session.execute(o_select2.bind(C_W_ID, C_D_ID,largestOrder));
		
		for (Row row : result) {
			System.out.println("Order number: "+largestOrder+" Entery Date: "+row.getTimestamp("o_entry_D")
					+" Carrier Id"+ row.getInt("o_carrier_id"));
		}
		
		//3. Each items in the last orders
		BoundStatement ol_select = new BoundStatement(orderst_OL_select);
		ResultSet orderLineResult = session.execute(ol_select.bind(C_W_ID, C_D_ID, largestOrder));
		
		for (Row row : orderLineResult) {
			System.out.println("Item numer: " + row.getInt("ol_i_id")
					+"Supplying warehouse number: "+ row.getInt("OL_SUPPLY_W_ID")
					+"Quantity ordered:" + row.getDouble("OL_QUANTITY")
					+"Total price for ordered item: "+ row.getDouble("OL_AMOUNT")
					+"Data and time of delivery: "+row.getTimestamp("OL_DELIVERY_D"));		
		}
	}
	
	
//	public static void main(String[] args) {
//		
//		connect = new CassandraConnect("localhost", 9042, "team3");
//		OrderStatus d = new OrderStatus(connect);
//		d.processOrderStatus(1, 1, 2231);
//		
//		connect.close();
//	}
	
}

