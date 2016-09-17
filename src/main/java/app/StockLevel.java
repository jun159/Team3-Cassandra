/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

import java.util.List;

import com.datastax.driver.core.*;

public class StockLevel {
	private static final String MESSAGE_ITEM_BELOW_THRESHOLD = "Total number of item below threshold (%d) : %d";
	private static final String DISTRICT_NEXT_AVAILABLE_O_ID = 
			  "SELECT d_next_o_id"
			+ "FROM district"
			+ "WHERE d_w_id = ? and d_id = ?"
			+ "ORDER BY d_next_o_id DESC"
			+ "LIMIT 1;";
	
	private Session session;
	private PreparedStatement nextAvailableOrderNum_Select;
	private Row targetDistrict;
	
	public StockLevel(CassandraConnect connect) {
		this.session = connect.getSession();
		this.nextAvailableOrderNum_Select = session.prepare(DISTRICT_NEXT_AVAILABLE_O_ID);
	}
	
	//====================================================================================
	// Processing for StockLevel transaction
	//====================================================================================
	
	public void processStockLevel(int w_id, int d_id, int stockThreshold, int numOfLastOrder) {
		int nextAvailableOrderID = getNextAvailableOrderNum(w_id, d_id);
		int numOfItemBelowThreshold = countItemBelowThreshold(nextAvailableOrderID);
		printTotalNumBelowThreshold(stockThreshold, numOfItemBelowThreshold);
	}
		
	//====================================================================================
	// CQL: Retrieve next available order number 'D_NEXT_O_ID' for district (W ID,D ID)
	//====================================================================================
	public int getNextAvailableOrderNum(int w_id, int d_id) {
		int nextAvailableNum = 0;
		ResultSet resultSet = session.execute(nextAvailableOrderNum_Select.bind(w_id, d_id));
		List<Row> district = resultSet.all();
		
		if(!district.isEmpty()) {
			targetDistrict = district.get(0);
			nextAvailableNum = targetDistrict.getInt("d_next_o_id");
		}
		
		return nextAvailableNum;
	}
	
	//=====================================================================================
	// CQL: Retrieve set of items from last L orders for district (W ID,D ID)
	//=====================================================================================
	public int countItemBelowThreshold(int nextAvailableOrderID) {
		int countBelowThreshold = 0;
		return countBelowThreshold;
	}
	
	public void printTotalNumBelowThreshold(int stockThreshold, int numOfItemBelowThreshold) {
		System.out.println(String.format(MESSAGE_ITEM_BELOW_THRESHOLD, stockThreshold, numOfItemBelowThreshold));
	}
	
}
