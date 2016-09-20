/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PopularItem {

	private static final String MESSAGE_DISTRICT_IDENTIFIER = "District Identifier : w_id = %d, d_id = %d";
	private static final String MESSAGE_NUM_LAST_ORDER = "Number of last orders examined: %d";
	private static final String MESSAGE_ORDER_ID_AND_DATE_TIME = "Order ID: %d, Date and Time: %s";
	private static final String MESSAGE_CUSTOMER_NAME = "Customer Name : %s";
	private static final String MESSAGE_POPULAR_ITEM = "Item name: %s, OL_Quantity = %2f";
	private static final String MESSAGE_PERCENTAGE_OF_ORDER_CONTAINING_POPULAR_ITEM = "Item name: %s, %.2f percent of orders contains item.";
	
	// ArrayLists: (1) name of popular item (2) number of orders containing the item.
	private ArrayList<String> nameOfPopularItem;
	private ArrayList<Integer> countOrder;
	
	//====================================================================================
	// CQL Queries for Popular Item transactions
	//====================================================================================
	private static final String DISTRICT_NEXT_AVAILABLE_O_ID = 
			          "SELECT d_next_o_id "
					+ "FROM district "
					+ "WHERE d_w_id = ? and d_id = ? ; ";
	
	private static final String SELECT_LAST_ORDER = 
					  "SELECT o_id, o_c_id, o_entry_d "
					+ "FROM orders "
					+ "WHERE o_d_id = ? "
					+ "AND o_w_id = ? "
					+ "AND o_id >= ? and o_id < ? ; ";
	
	private static final String ORDERLINE_FOR_AN_ORDER = 
					  "SELECT *"
					+ "FROM orderline "
					+ "WHERE ol_o_id = ? "
					+ "AND ol_d_id = ? "
					+ "AND ol_w_id = ? ; ";
	
	private static final String CUSTOMER_NAME = 
			          "SELECT c_first, c_middle, c_last "
					+ "FROM customer "
					+ "WHERE c_w_id = ? "
					+ "and c_d_id = ? "
					+ "and c_id >= ? ;";
	
	private static final String ITEM_NAME = 
			          "SELECT i_name "
					+ "FROM item "
					+ "WHERE i_id = ?;";
	
	//====================================================================================
	// Preparing for session
	//====================================================================================

	private Session session;
	
	private PreparedStatement nextAvailableOrderNum_Select;
	private PreparedStatement getLastOrder_Select;
	private PreparedStatement orderLine_Select;
	private PreparedStatement customerName_Select;
	private PreparedStatement itemName_Select;
	
	private Row targetDistrict;
	private Row targetOrder;
	private Row targetOrderline;
	private Row targetCustomer;
	private Row targetItem;

	public PopularItem(CassandraConnect connect) {
		this.session = connect.getSession();
		this.nextAvailableOrderNum_Select = session.prepare(DISTRICT_NEXT_AVAILABLE_O_ID);
		this.getLastOrder_Select = session.prepare(SELECT_LAST_ORDER);
		this.orderLine_Select = session.prepare(ORDERLINE_FOR_AN_ORDER);
		this.customerName_Select = session.prepare(CUSTOMER_NAME);
		this.itemName_Select = session.prepare(ITEM_NAME);
	}

	//====================================================================================
	// Processing Popular Item transaction
	//====================================================================================

	public void processPopularItem(int w_id, int d_id, int numOfLastOrder) {

		printDistrictIdentifierAndNumOfLastOrder(w_id, d_id, numOfLastOrder);
		
		int nextOrderID = getNextAvailableOrderNum(w_id, d_id);
		
		List<Row> setOfLastOrder = getLastOrder(d_id, w_id, nextOrderID, numOfLastOrder);
		processOrderForPopularItem(setOfLastOrder, w_id, d_id);
	}
	
	public void processOrderForPopularItem(List<Row> setOfLastOrder, int w_id, int d_id) {
		int o_id, c_id;
		Date date_and_time;
		
		// Processing an order
		
		for(int i = 0; i < setOfLastOrder.size(); i++) {
			
			targetOrder =  setOfLastOrder.get(i);
			o_id = targetOrder.getInt("o_id");
			c_id = targetOrder.getInt("o_c_id");
			date_and_time = targetOrder.getTimestamp("o_entry_d");
			
			printOrderDetail(o_id, date_and_time.toString());
			printCustomerName(getCustomerName(w_id, d_id, c_id));
			
			// Get the orderlines of this order
			List<Row> setOfOrderline = getOrderLine(o_id, d_id, w_id);
			//findPopularItem(setOfOrderline);
		}
		
		findPercentageOfOrder(setOfLastOrder.size());
	}

	private void findPercentageOfOrder(int totalOrderSize) {
		float percentage;
		for(int i = 0; i < nameOfPopularItem.size(); i++) {
			percentage = (countOrder.get(i) / totalOrderSize) * 100;
			printPercentageOrder(nameOfPopularItem.get(i), percentage);
		}
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
	// CQL: Retrieve set of last L orders in Order table for district (W ID,D ID)
	//=====================================================================================
	private List<Row> getLastOrder(int d_id, int w_id, int nextAvailableOrderID, int numOfLastOrder) {
		int startingOrderID = nextAvailableOrderID - numOfLastOrder;
		
		System.out.println("Finding all items from " + startingOrderID + " to " + nextAvailableOrderID +"......");
		
		ResultSet resultSet = session.execute(getLastOrder_Select.bind(d_id, w_id, startingOrderID, nextAvailableOrderID));
		List<Row> items = resultSet.all();
		return items;
	}
	
	//=====================================================================================
	// CQL: Retrieve a customer name from the Customer Table
	//=====================================================================================
	public String getCustomerName(int w_id, int d_id, int c_id) {
		
		ResultSet resultSet = session.execute(customerName_Select.bind(w_id, d_id, c_id));
		List<Row> customer = resultSet.all();
		targetCustomer = customer.get(0);
		return targetCustomer.getString("c_first") + " " + targetCustomer.getString("c_middle") + " " + targetCustomer.getString("c_last");

	}
	
	//=====================================================================================
	// CQL: Retrieve the set of order-lines for an order 'o_id'
	//=====================================================================================
	public List<Row> getOrderLine(int o_id, int d_id, int w_id) {

		ResultSet resultSet = session.execute(orderLine_Select.bind(o_id, d_id, w_id));
		List<Row> orderline = resultSet.all();
		return orderline;

	}
	
	//=====================================================================================
	// Retrieve popular item/s in a set of orderline
	//=====================================================================================
	public void findPopularItem(List<Row> setOfOrderline) {
		
		// Find largest quantity in orderline (assuming it is arranged by database)
		targetItem = setOfOrderline.get(0);
		BigDecimal max_ol_quantity = targetItem.getDecimal("ol_quantity");
		int i_id;
		String itemName;
		
		// Iterate through orderline to get all popular items
		for(int i = 0; i < setOfOrderline.size(); i++) {
			targetOrderline = setOfOrderline.get(i);
			if(targetOrderline.getDecimal("ol_quantity").compareTo(max_ol_quantity) == 1){
				break;
			}
			
			// Retrieve item ID and item name
			i_id = targetOrderline.getInt("ol_i_id");
			itemName = getItemName(i_id);
			
			// Record the popular item into item list
			if(nameOfPopularItem.contains(itemName)) {
				insertIntoPopularItemList(itemName);
			}
			
			else {
				updatePopularItemList(itemName);
			}
			
			printPopularItem(itemName, max_ol_quantity);
		}

	}

	private void insertIntoPopularItemList(String itemName) {
		int index;
		index = nameOfPopularItem.indexOf(itemName);
		countOrder.set(index, countOrder.get(index) + 1);
	}
	
	public void updatePopularItemList(String itemName){
		nameOfPopularItem.add(itemName);
		countOrder.add(1);
	}
	
	//=====================================================================================
	// CQL: Retrieve an item name from the Item Table
	//=====================================================================================
	public String getItemName(int item_ID) {

		ResultSet resultSet = session.execute(itemName_Select.bind(item_ID));
		List<Row> item = resultSet.all();

		if(!item.isEmpty()) {
			targetItem = item.get(0);
			return targetItem.getString("i_name");
		}

		return null;
	}
	
	//=====================================================================================
	// For printing outputs
	//=====================================================================================

	public void printDistrictIdentifierAndNumOfLastOrder(int w_id, int d_id, int numOfLastOrder) {
		System.out.println(String.format(MESSAGE_DISTRICT_IDENTIFIER, w_id, d_id));
		System.out.println(String.format(MESSAGE_NUM_LAST_ORDER, numOfLastOrder));
	}
	
	public void printOrderDetail(int o_id, String dateTime) {
		System.out.println(String.format(MESSAGE_ORDER_ID_AND_DATE_TIME, o_id, dateTime));
	}
	
	public void printCustomerName(String name) {
		System.out.println(String.format(MESSAGE_CUSTOMER_NAME, name));
	}
	
	public void printPopularItem(String itemName, BigDecimal max_ol_quantity) {
		System.out.println(String.format(MESSAGE_POPULAR_ITEM, itemName, max_ol_quantity));
	}
	
	public void printPercentageOrder(String itemName, float percentage) {
		System.out.println(String.format(MESSAGE_PERCENTAGE_OF_ORDER_CONTAINING_POPULAR_ITEM, itemName, percentage));
	}
	
}
