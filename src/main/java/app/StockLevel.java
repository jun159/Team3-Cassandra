/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

import java.util.List;

import com.datastax.driver.core.*;

public class StockLevel {
	
	private static final String MESSAGE_ITEM_BELOW_THRESHOLD = "Total number of item below threshold (%d) : %d";

	//====================================================================================
	// CQL Queries for StockLevel transactions
	//====================================================================================
	private static final String DISTRICT_NEXT_AVAILABLE_O_ID = 
					  "SELECT d_next_o_id "
					+ "FROM district "
					+ "WHERE d_w_id = ? and d_id = ? "
					+ "ORDER BY d_next_o_id DESC "
					+ "LIMIT 1;";
	private static final String ITEMS_IN_LAST_ORDER = 
			          "SELECT ol_i_id "
					+ "FROM orderline "
					+ "WHERE ol_d_id = ? "
					+ "and ol_w_id = ? "
					+ "and ol_o_id >= ? "
					+ "and ol_o_id < ?;";
	private static final String COUNT_ITEM_BELOW_THRESHOLD = 
			          "SELECT COUNT(*) "
					+ "FROM stock "
					+ "WHERE s_w_id = ? "
					+ "and s_i_id = ? "
					+ "and s_quantity < ?;";

	private Session session;
	private PreparedStatement nextAvailableOrderNum_Select;
	private PreparedStatement itemsInLastOrder_Select;
	private PreparedStatement countBelowThreshold_Select;
	private Row targetDistrict;
	private Row targetItemID;
	private Row targetStock;
	
	//====================================================================================
	// Preparing for session
	//====================================================================================

	public StockLevel(CassandraConnect connect) {
		this.session = connect.getSession();
		this.nextAvailableOrderNum_Select = session.prepare(DISTRICT_NEXT_AVAILABLE_O_ID);
		this.itemsInLastOrder_Select = session.prepare(ITEMS_IN_LAST_ORDER);
		this.countBelowThreshold_Select = session.prepare(COUNT_ITEM_BELOW_THRESHOLD);
	}

	//====================================================================================
	// Processing for StockLevel transaction
	//====================================================================================

	public void processStockLevel(int w_id, int d_id, int stockThreshold, int numOfLastOrder) {
		int nextAvailableOrderID = getNextAvailableOrderNum(w_id, d_id);
		int numOfItemBelowThreshold = countItemBelowThreshold(d_id, w_id, nextAvailableOrderID, numOfLastOrder, stockThreshold);
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
	public int countItemBelowThreshold(int d_id, int w_id, int nextAvailableOrderID, int numOfLastOrder, int stockThreshold) {
		int countBelowThreshold = 0;
		int startingOrderID = nextAvailableOrderID - numOfLastOrder;

		ResultSet resultSet = session.execute(itemsInLastOrder_Select.bind(d_id, w_id, startingOrderID, nextAvailableOrderID));
		List<Row> items = resultSet.all();

		if(!items.isEmpty()) {
			int ol_i_id;

			for(int i = 0; i < items.size(); i++) {
				targetItemID = items.get(i);
				ol_i_id = targetItemID.getInt("ol_i_id");

				resultSet = session.execute(countBelowThreshold_Select.bind(w_id, ol_i_id, stockThreshold));
				List<Row> stock = resultSet.all();	

				if(!stock.isEmpty()) {
					targetStock = stock.get(0);
					countBelowThreshold += targetStock.getInt("count");
				}
			}
		}

		return countBelowThreshold;
	}

	public void printTotalNumBelowThreshold(int stockThreshold, int numOfItemBelowThreshold) {
		System.out.println(String.format(MESSAGE_ITEM_BELOW_THRESHOLD, stockThreshold, numOfItemBelowThreshold));
	}

}
