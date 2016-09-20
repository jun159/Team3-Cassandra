package app;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.*;
public class Delivery {
		private static CassandraConnect connect;
		private Session session;
		
		private PreparedStatement orderSelect;
		private PreparedStatement orderSelect_o_c_id;	
		private PreparedStatement orderUpdate;
		private PreparedStatement orderLineSelect_ol_number;
		private PreparedStatement orderLineUpdate;
		private PreparedStatement orderLineSelect;
		private PreparedStatement customerSelect;
		private PreparedStatement customerUpdate;
		
		
		
		public Delivery(CassandraConnect connect) {
			this.session=connect.getSession();
		
			/* Problem here is the not the samllest o_id*/
			this.orderSelect = session.prepare("SELECT o_id,o_carrier_id FROM " 
					 + "Orders WHERE O_W_ID = ? AND O_D_ID = ?;");
			
			this.orderSelect_o_c_id = session.prepare("SELECT o_c_id FROM " 
					 + "Orders WHERE o_w_id =? AND o_d_id = ? AND o_id =?;");
			
			this.orderUpdate = session.prepare("UPDATE " 
					 + "Orders SET o_carrier_id = ? WHERE o_w_id =? AND o_d_id = ? AND o_id =?;");
			
			
			this.orderLineSelect_ol_number = session.prepare("Select ol_number from " 
					 + "OrderLine WHERE ol_w_id =? AND ol_d_id=? AND ol_o_id =?;");
			
			
			this.orderLineUpdate = session.prepare("UPDATE " 
					 + "OrderLine SET OL_DELIVERY_D = dateOf(now()) WHERE ol_w_id =? AND ol_d_id=? AND ol_o_id =? AND ol_number =?;");
			
			
			this.orderLineSelect = session.prepare("SELECT ol_amount FROM " 
					 + "OrderLine WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id =?;");
				
			this.customerSelect = session.prepare("SELECT c_balance,c_delivery_cnt FROM " 
					 + "customer WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?;");
			
			this.customerUpdate = session.prepare("UPDATE " 
					 + "customer SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");

		}
		
		
		public void processDelivery(int W_ID, int CARRIER_ID){
	
			for (int i = 1; i <= 10; i++) {  //i is the D_ID
				BoundStatement o_select1 = new BoundStatement(orderSelect);
				ResultSet results = session.execute(o_select1.bind(W_ID, i));
				int o_id = -1;  //Default is DESC
				for (Row row : results) {
					if(row.getInt("o_carrier_id")==-1)  //o_carrier_id null is -1
						o_id = row.getInt("o_id");
				}
				
				
				//b) update Orders table from o_carrier_id to CARRIER_ID
				BoundStatement O_update = new BoundStatement(orderUpdate);
				session.execute(O_update.bind(CARRIER_ID, W_ID, i, o_id));
				
				
				//c) update OL table, set OL_DELIVERY_D to now()
				BoundStatement OL_select_ol_number = new BoundStatement(orderLineSelect_ol_number);
				results = session.execute(OL_select_ol_number.bind(W_ID,i,o_id));
				
				for(Row row : results){
					BoundStatement OL_update = new BoundStatement(orderLineUpdate);
					session.execute(OL_update.bind(W_ID, i, o_id, row.getInt("ol_number")));
				}
				
				
				//d) Update Customers
				//get the customer from order table
				BoundStatement O_select = new BoundStatement(orderSelect_o_c_id);
				results = session.execute(O_select.bind(W_ID,i,o_id));
				
				int c_id = -1;
				for (Row row : results) {
					c_id = row.getInt("o_c_id");
				}
				
				//get B from orderLine table
				BoundStatement OL_select = new BoundStatement(orderLineSelect);
				results = session.execute(OL_select.bind(o_id,W_ID,i));
				
				BigDecimal B = BigDecimal.valueOf(0);
				for (Row row : results) {
					B.add(row.getDecimal("ol_amount"));
//					B = B +  row.getDouble("ol_amount");
				}
				
				//get c_balance from customer before update
				BoundStatement C_select = new BoundStatement(customerSelect);
				session.execute(C_select.bind(c_id, W_ID, i));
				
				int c_delivery_cnt = 0;
				for (Row row : results) {
					c_delivery_cnt = row.getInt("c_delivery_cnt");
					B.add(row.getDecimal("c_balance"));
				}
				
				//update the customer
				BoundStatement C_update = new BoundStatement(customerUpdate);
				results = session.execute(C_update.bind(B,c_delivery_cnt,W_ID,i,c_id));
			}
			
			System.out.println("Delivery done successfully.");
		}
	
		
		
		public static void main(String[] args) {
			
//			connect = new CassandraConnect("localhost", 9042, "team3");
//			Delivery d = new Delivery(connect);
//			d.processDelivery(1, 2);
//			
//			connect.close();
		}
		
}