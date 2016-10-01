/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

import java.util.List;

import com.datastax.driver.core.*;

public class StockLevel {
	
	private static final String MESSAGE_ITEM_BELOW_THRESHOLD = "For last '%d' orders in (w_id = %d, d_id = %d), num of items below threshold (%d) : %d\n";

	//====================================================================================
	// CQL Queries for StockLevel transactions
	//====================================================================================
	private static final String DISTRICT_NEXT_AVAILABLE_O_ID = 
					  "SELECT d_next_o_id "
					+ "FROM district "
					+ "WHERE d_w_id = ? and d_id = ?;";
	
	private static final String ITEMS_IN_LAST_ORDER = 
			          "SELECT ol_i_id "
					+ "FROM orderline "
					+ "WHERE ol_d_id = ? "
					+ "AND ol_w_id = ? "
					+ "AND ol_o_id >= ? and ol_o_id < ?";
	
	private static final String COUNT_ITEM_BELOW_THRESHOLD = 
			          "SELECT COUNT(*) "
					+ "FROM stock "
					+ "WHERE s_w_id = ? "
					+ "and s_i_id = ? "
					+ "and s_quantity < ? "
					+ "ALLOW FILTERING;";
	
	private static final String TEST_ITEM_S_QUANTITY_BELOW_THRESHOLD = 
	          "SELECT s_quantity "
			+ "FROM stock "
			+ "WHERE s_w_id = ? "
			+ "and s_i_id = ? "
			+ "and s_quantity < ? "
			+ "ALLOW FILTERING;";

	private Session session;
	
	private PreparedStatement nextAvailableOrderNum_Select;
	private PreparedStatement itemsInLastOrder_Select;
	private PreparedStatement countBelowThreshold_Select;
	private PreparedStatement testItemQuantity;
	
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
		this.testItemQuantity = session.prepare(TEST_ITEM_S_QUANTITY_BELOW_THRESHOLD);
		this.countBelowThreshold_Select = session.prepare(COUNT_ITEM_BELOW_THRESHOLD);
	}

	//====================================================================================
	// Processing for StockLevel transaction
	//====================================================================================

	public void processStockLevel(int w_id, int d_id, int stockThreshold, int numOfLastOrder) {
		int nextAvailableOrderID = getNextAvailableOrderNum(w_id, d_id);
		//System.out.println("Next Order Number : " + nextAvailableOrderID);
		
		long numOfItemBelowThreshold = countItemBelowThreshold(d_id, w_id, nextAvailableOrderID, numOfLastOrder, stockThreshold);
		
		printTotalNumBelowThreshold(numOfLastOrder, w_id, d_id, stockThreshold, numOfItemBelowThreshold);
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
	// CQL: Count the number of items below the given threshold T
	//=====================================================================================
	public Long countItemBelowThreshold(int d_id, int w_id, int nextAvailableOrderID, int numOfLastOrder, int stockThreshold) {
		Long countBelowThreshold = new Long(0);
		Double threshold = new Double(stockThreshold);
		
		
		ResultSet resultSet;
		List<Row> items = getItemsInLastOrder(d_id, w_id, nextAvailableOrderID, numOfLastOrder);
		
		if(!items.isEmpty()) {
			int ol_i_id;

			for(int i = 0; i < items.size(); i++) {
				// countBelowThreshold = testCountItemBelowThreshold(w_id, countBelowThreshold, threshold, items, i);
				targetItemID = items.get(i);
				ol_i_id = targetItemID.getInt("ol_i_id");
				
				resultSet = session.execute(countBelowThreshold_Select.bind(w_id, ol_i_id, threshold));
				List<Row> stock = resultSet.all();
				if(!stock.isEmpty()) {
					targetStock = stock.get(0);
					countBelowThreshold += targetStock.getLong("Count");
				}
			}
		}

		return countBelowThreshold;
	}

	@SuppressWarnings("unused")
	private Long testCountItemBelowThreshold(int w_id, Long countBelowThreshold, Double threshold, List<Row> items, int i) {
		ResultSet resultSet;
		int ol_i_id_test;
		targetItemID = items.get(i);
		ol_i_id_test = targetItemID.getInt("ol_i_id");
		
		resultSet = session.execute(testItemQuantity.bind(w_id, ol_i_id_test, threshold));
		List<Row> stock = resultSet.all();
		if(!stock.isEmpty()) {
			targetStock = stock.get(0);
			System.out.println("Item ID: " + ol_i_id_test + ", s_quantity = " + targetStock.getDouble("s_quantity"));
			countBelowThreshold += 1;
		}
		return countBelowThreshold;
	}

	//=====================================================================================
	// CQL: Retrieve set of items from last L orders for district (W ID,D ID)
	//=====================================================================================

	private List<Row> getItemsInLastOrder(int d_id, int w_id, int nextAvailableOrderID, int numOfLastOrder) {
		int startingOrderID = nextAvailableOrderID - numOfLastOrder;
		
		System.out.println("Finding all items from " + startingOrderID + " to " + nextAvailableOrderID +"......");
		
		ResultSet resultSet = session.execute(itemsInLastOrder_Select.bind(d_id, w_id, startingOrderID, nextAvailableOrderID));
		List<Row> items = resultSet.all();
		return items;
	}

	public void printTotalNumBelowThreshold(int numOfLastOrder, int w_id, int d_id, int stockThreshold, long numOfItemBelowThreshold) {
		System.out.println(String.format(MESSAGE_ITEM_BELOW_THRESHOLD, numOfLastOrder, w_id, d_id, stockThreshold, numOfItemBelowThreshold));
	}

}
