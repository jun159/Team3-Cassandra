/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PopularItem {

	private static final String MESSAGE_DISTRICT_IDENTIFIER = "District Identifier : w_id = %d, d_id = %d";
	private static final String MESSAGE_NUM_LAST_ORDER = "Number of last orders examined: %d";
	private static final String MESSAGE_ORDER_ID_AND_DATE_TIME = "Order ID: %d, Date and Time: %s";
	private static final String MESSAGE_CUSTOMER_NAME = "Customer Name : %s %s %s";
	private static final String MESSAGE_POPULAR_ITEM = "Item name: %s, OL_Quantity = %d";
	private static final String MESSAGE_PERCENTAGE_OF_ORDER_CONTAINING_POPULAR_ITEM = "Item name: %s, Percentage of Orders containing item: %.2f";

	private int w_id;
	private int d_id;
	private int numOfLastOrder;

	//====================================================================================
	// CQL Queries for Popular Item transactions
	//====================================================================================
	private static final String DISTRICT_NEXT_AVAILABLE_O_ID = 
			          "SELECT d_next_o_id "
					+ "FROM district"
					+ "WHERE d_w_id = ? and d_id = ? "
					+ "ORDER BY d_next_o_id DESC "
					+ "LIMIT 1;";
	private static final String ITEMS_IN_LAST_ORDER = 
			          "SELECT ol_i_id"
					+ "FROM order"
					+ "WHERE o_d_id = ? "
					+ "and o_w_id = ? "
					+ "and o_id >= ?"
					+ "and o_id < ?;";
	private static final String ORDERLINE_FOR_AN_ORDER = 
					  "SELECT *"
					+ "FROM orderline"
					+ "WHERE ol_o_id = ? "
					+ "and o_d_id = ? "
					+ "and o_w_id >= ?;";
	
	//====================================================================================
	// Preparing for session
	//====================================================================================

	private Session session;
	private PreparedStatement nextOrderNum_Select;
	private PreparedStatement itemsInLastOrder_Select;
	private PreparedStatement orderLine_Select;
	private Row targetDistrict;

	public PopularItem(CassandraConnect connect) {
		this.session = connect.getSession();
		this.nextOrderNum_Select = session.prepare(DISTRICT_NEXT_AVAILABLE_O_ID);
		this.itemsInLastOrder_Select = session.prepare(ITEMS_IN_LAST_ORDER);
		this.orderLine_Select = session.prepare(ORDERLINE_FOR_AN_ORDER);
	}

	//====================================================================================
	// Processing Popular Item transaction
	//====================================================================================

	public void processPopularItem(int w_id, int d_id, int numOfLastOrder) {
		w_id = this.w_id;
		d_id = this.d_id;
		numOfLastOrder = this.numOfLastOrder;
		printMostPopularItem();
		int nextOrderID = getNextAvailableOrderNum(w_id, d_id);
		List<Row> setOfLastOrder = getLastOrder(d_id, w_id, nextOrderID, numOfLastOrder);
	}

	//====================================================================================
	// CQL: Retrieve next available order number 'D_NEXT_O_ID' for district (W ID,D ID)
	//====================================================================================
	public int getNextAvailableOrderNum(int w_id, int d_id) {
		int nextAvailableNum = 0;
		ResultSet resultSet = session.execute(nextOrderNum_Select.bind(w_id, d_id));
		List<Row> district = resultSet.all();

		if(!district.isEmpty()) {
			targetDistrict = district.get(0);
			nextAvailableNum = targetDistrict.getInt("d_next_o_id");
		}

		return nextAvailableNum;
	}

	//=====================================================================================
	// CQL: Retrieve set of items from last L orders in Order table for district (W ID,D ID)
	//=====================================================================================
	public List<Row> getLastOrder(int d_id, int w_id, int nextOrderID, int numOfLastOrder) {
		int startingOrderID = nextOrderID - numOfLastOrder;

		ResultSet resultSet = session.execute(itemsInLastOrder_Select.bind(d_id, w_id, startingOrderID, nextOrderID));
		List<Row> items = resultSet.all();
		
		if(!items.isEmpty()) {
			return items;
		}
		
		return null;
	}
	
	//=====================================================================================
	// CQL: Retrieve the set of order-lines for an order 'o_id'
	//=====================================================================================
	public List<Row> getOrderLine(int o_id, int d_id, int w_id) {

		ResultSet resultSet = session.execute(orderLine_Select.bind(o_id, d_id, w_id));
		List<Row> orderline = resultSet.all();
		
		if(!orderline.isEmpty()) {
			return orderline;
		}
		
		return null;
	}

	public void printMostPopularItem() {
		System.out.println(String.format(MESSAGE_DISTRICT_IDENTIFIER, w_id, d_id));
		System.out.println(String.format(MESSAGE_NUM_LAST_ORDER, numOfLastOrder));
	}
}
