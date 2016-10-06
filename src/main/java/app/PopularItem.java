/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PopularItem {
	
	private static final String MESSAGE_PROCESSING = "\n********Find the popular items for last %d orders at warehouse district { %d, %d}********\n";
	private static final String MESSAGE_DISTRICT_IDENTIFIER = "District Identifier : w_id = %d, d_id = %d";
	private static final String MESSAGE_NUM_LAST_ORDER = "Number of last orders examined: %d";
	private static final String MESSAGE_ORDER_ID_AND_DATE_TIME = "\nOrder ID: %d, Date and Time: %s";
	private static final String MESSAGE_CUSTOMER_NAME = "Customer Name : %s";
	private static final String MESSAGE_POPULAR_ITEM = "{Item name: %s, OL_Quantity = %s}";
	private static final String MESSAGE_PERCENTAGE_OF_ORDER_CONTAINING_POPULAR_ITEM = "%.2f%% of orders contains item '%s'";
	
	// ArrayLists: (1) name of popular item (2) number of orders containing the item.
	private ArrayList<String> distinctItemArrayList;
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
	
	private static final String MAX_OL_QUANTITY_IN_AN_ORDERLINE = 
					  "SELECT max(ol_quantity) as max_quantity "
					+ "FROM orderline "
					+ "WHERE ol_o_id = ?; ";
	
	private static final String RETRIEVE_POPULAR_ITEM = 
					  "SELECT ol_i_id, ol_quantity "
					+ "FROM orderline "
					+ "WHERE ol_o_id = ? "
					+ "AND ol_quantity = ? "
					+ "ALLOW FILTERING ; ";
	
	private static final String CUSTOMER_NAME = 
			          "SELECT c_first, c_middle, c_last "
					+ "FROM customer "
					+ "WHERE c_w_id = ? "
					+ "and c_d_id = ? "
					+ "and c_id >= ? ;";
	
	private static final String ITEM_NAME = 
			          "SELECT i_name "
					+ "FROM stockitem "
					+ "WHERE s_i_id = ? "
					+ "ALLOW FILTERING ;";
	
	//====================================================================================
	// Preparing for session
	//====================================================================================

	private Session session;
	
	private PreparedStatement nextAvailableOrderNum_Select;
	private PreparedStatement getLastOrder_Select;
	private PreparedStatement getMaxOrderlineQuantity;
	private PreparedStatement selectPopularItemFromOrderLine;
	private PreparedStatement customerName_Select;
	private PreparedStatement itemName_Select;
	
	private Row targetDistrict;
	private Row targetOrder;
	private Row targetCustomer;
	private Row targetItem;
	private Row targetMax;
	

	public PopularItem(CassandraConnect connect) {
		this.session = connect.getSession();
		
		this.nextAvailableOrderNum_Select = session.prepare(DISTRICT_NEXT_AVAILABLE_O_ID);
		this.getLastOrder_Select = session.prepare(SELECT_LAST_ORDER);
		
		this.getMaxOrderlineQuantity = session.prepare(MAX_OL_QUANTITY_IN_AN_ORDERLINE);
		this.selectPopularItemFromOrderLine = session.prepare(RETRIEVE_POPULAR_ITEM);
		
		this.customerName_Select = session.prepare(CUSTOMER_NAME);
		this.itemName_Select = session.prepare(ITEM_NAME);
	}

	//====================================================================================
	// Processing Popular Item transaction
	//====================================================================================

	public void processPopularItem(int w_id, int d_id, int numOfLastOrder) {
		
		System.out.printf(MESSAGE_PROCESSING, numOfLastOrder, w_id, d_id);
		distinctItemArrayList = new ArrayList<String>();
		countOrder = new ArrayList<Integer>();

		printDistrictIdentifierAndNumOfLastOrder(w_id, d_id, numOfLastOrder);
		
		int nextOrderID = getNextAvailableOrderNum(w_id, d_id);
		
		List<Row> setOfLastOrder = getLastOrder(d_id, w_id, nextOrderID, numOfLastOrder);
		
		processOrderForPopularItem(setOfLastOrder, w_id, d_id);
		
		findPercentageOfOrder(numOfLastOrder);
	}
	
	public void processOrderForPopularItem(List<Row> setOfLastOrder, int w_id, int d_id) {
		int o_id, c_id;
		Date date_and_time;
		
		// Processing set of orders
		
		for(int i = 0; i < setOfLastOrder.size(); i++) {
			
			targetOrder =  setOfLastOrder.get(i);
			o_id = targetOrder.getInt("o_id");
			c_id = targetOrder.getInt("o_c_id");
			date_and_time = targetOrder.getTimestamp("o_entry_d");
			
			printOrderDetail(o_id, date_and_time.toString());
			printCustomerName(getCustomerName(w_id, d_id, c_id));
			
			// Find the popular items by identifying the max ol_quantity
			findPopularItem(o_id);
		}
		
	}

	private void findPercentageOfOrder(int totalOrderSize) {
		float percentage;
		System.out.println();
		
		for(int i = 0; i < distinctItemArrayList.size(); i++) {
			percentage = ( (float) countOrder.get(i) / (float) totalOrderSize) * 100;
			printPercentageOrder(distinctItemArrayList.get(i), percentage);
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
		
		System.out.println("Find orders from " + startingOrderID + " to " + nextAvailableOrderID +"......");
		
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
	// Retrieve popular item/s in a set of orderline
	//=====================================================================================
	public void findPopularItem(int o_id) {
		
		// Find max quantity in this set of orderline 
		Double max_ol_quantity = getMaxOL_Quantity(o_id);
		
		// Get popular items (with max_ol_quantity) from orderline table
		ResultSet resultSet = session.execute(selectPopularItemFromOrderLine.bind(o_id, max_ol_quantity));
		List<Row> setOfPopularItem = resultSet.all();
		
		String itemName;
		int itemID;
		
		// Output popular items {name, ol_quantity}
		for(int i = 0; i < setOfPopularItem.size(); i++) {
			targetItem = setOfPopularItem.get(i);
			itemID = targetItem.getInt("ol_i_id");
			itemName = getItemName(itemID);
			printPopularItem(itemName, max_ol_quantity);
			addToDistinctItemArrayList(itemName);
		}
	}

	private Double getMaxOL_Quantity(int o_id) {
		
		// Get the max ol_quantity from the set of orderline in this order
		ResultSet resultSet = session.execute(getMaxOrderlineQuantity.bind(o_id));
		List<Row> maxOrderlineQuantity = resultSet.all();
		
		//Retrieve the max_ol_quantity
		targetMax = maxOrderlineQuantity.get(0);
		Double max_ol_quantity = targetMax.getDouble("max_quantity");
		
		return max_ol_quantity;
	}
	
	private void addToDistinctItemArrayList(String itemName) {
		if(distinctItemArrayList.contains(itemName)) {
			updatePopularItemList(itemName);
		}
		else {
			insertIntoPopularItemList(itemName);
		}
	}

	private void insertIntoPopularItemList(String itemName) {
		distinctItemArrayList.add(itemName);
		countOrder.add(1);
	}
	
	public void updatePopularItemList(String itemName){
		int index;
		index = distinctItemArrayList.indexOf(itemName);
		countOrder.set(index, countOrder.get(index) + 1);
	}
	
	//=====================================================================================
	// CQL: Retrieve an item name from the StockItem Table
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
	// Methods for printing outputs
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
	
	public void printPopularItem(String itemName, Double max_ol_quantity) {
		System.out.println(String.format(MESSAGE_POPULAR_ITEM, itemName, max_ol_quantity.toString()));
	}
	
	public void printPercentageOrder(String itemName, float percentage) {
		System.out.println(String.format(MESSAGE_PERCENTAGE_OF_ORDER_CONTAINING_POPULAR_ITEM, percentage, itemName));
	}
	
}
