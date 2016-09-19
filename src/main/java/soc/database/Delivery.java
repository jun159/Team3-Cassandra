package soc.database;

import java.util.Date;
import java.util.List;

import com.datastax.driver.core.*;
public class Delivery {
		
		private Session session;
		private String keyspace;
	
		
		//select smallest o_d_id  and O_CARRIER_ID =null
		private static final String SELECT_ORDER = 
				"SELECT O_D_ID "
				+ "FROM ORDER "
				+ "WHERE o_w_id = ? AND 0_d_id=?";
		
		
		private PreparedStatement delivery_D_select;
		private PreparedStatement delivery_D_update;
		private PreparedStatement delivery_O_select;
		private PreparedStatement delivery_O_update;
		private PreparedStatement delivery_C_update;
		
		
		public void delivery(int w_id, int carrier_id){
			
			delivery_D_select = session.prepare("SELECT o_id FROM " 
					+ keyspace + ".delivery WHERE o_w_id = ? and o_d_id =? and o_carrier_id = 0 LIMIT 1;");
			
			delivery_O_select = session.prepare("SELECT c_id,ol_id,ol_amount FROM " 
					+ keyspace + ".orders WHERE o_w_id = ? and o_d_id =? and o_id = ? and o_carrier_id = 0;");
			
			delivery_O_update = session.prepare("UPDATE " 
					+ keyspace + ".orders SET o_carrier_id = ?, ol_delivery_d = dateOf(now()) WHERE o_w_id =? and o_d_id = ? and o_id =? and ol_id = ?;");
			
			delivery_D_update = session.prepare("UPDATE " 
					+ keyspace + ".delivery SET o_carrier_id = ? WHERE o_w_id =? and o_d_id = ? and o_id =?;");
			
			delivery_C_update = session.prepare("UPDATE " 
					+ keyspace + ".customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt+100 WHERE c_w_id = ? and c_d_id = ? and c_id = ?;");

			
			
			for (int i = 1; i <= 10; i++) {
				BoundStatement bound_D_sel = new BoundStatement(delivery_D_select);
				ResultSet results = session.execute(bound_D_sel.bind(w_id, i));
				int o_id = 0;
				for (Row row : results) {
					o_id = row.getInt("o_id");
				}
				//update delivery table for o_carrier_id
				BoundStatement bound_d_up = new BoundStatement(delivery_D_update);
				session.execute(bound_d_up.bind(carrier_id, w_id, i, o_id));
				
				// update order table for o_carrier_id
				BoundStatement bound_o_sel = new BoundStatement(delivery_O_select);
				results = session.execute(bound_o_sel.bind(w_id, i, o_id));
				
				double B = 0.0;
				int c_id =0;
				for (Row row : results) {
					c_id = row.getInt("c_id");
					int ol_id = row.getInt("ol_id");
					B = B +  row.getDouble("ol_amount");
					
					BoundStatement bound_o_up = new BoundStatement(delivery_O_update);
					session.execute(bound_o_up.bind(carrier_id, w_id, i,
									o_id, ol_id));
				}
				B = B / 100;
				BoundStatement bound_c_up = new BoundStatement(delivery_C_update);
				session.execute(bound_c_up.bind((long)B, w_id, i, c_id));
			}
			
			
		}
	
}
