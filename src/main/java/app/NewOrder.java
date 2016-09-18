/* Author: Luah Bao Jun
 * ID: A0126258A
 * Team: 3
 */

package app;

import java.util.Date;
import java.util.List;

import com.datastax.driver.core.*;

public class NewOrder {
	
	private static final String MESSAGE_CUSTOMER = "Customer information: ID(%1$s, %1$s, %1$s), LastName(%1$s), "
			+ "Credit(%1$s), Discount(%1$s)";
	private static final String MESSAGE_WAREHOUSE = "Warehouse tax rate: %1$s";
	private static final String MESSAGE_DISTRICT = "District tax rate: %1$s";
	private static final String MESSAGE_ORDER = "Order: OrderNumber(%1$s), EntryDate(%1$s)";
	private static final String MESSAGE_NUM_ITEMS = "Number of items: %1$s";
	private static final String MESSAGE_TOTAL_AMOUNT = "Total amount: %1$s";
	private static final String MESSAGE_ORDER_ITEM = "Order item: ItemNumber(%1$s), ItemName(%1$s), "
			+ "Warehouse(%1$s), Quantity(%1$s), Amount(%1$s), TotalQuantity(%1$s)";
	
	private static final String SELECT_WAREHOUSE = 
			"SELECT w_tax "
			+ "FROM warehouse "
			+ "WHERE w_id = ?";
	private static final String SELECT_DISTRICT =
			"SELECT d_next_o_id, d_tax "
			+ "FROM district "
			+ "WHERE w_id = ? "
			+ "AND d_id = ?";
	private static final String UPDATE_DISTRICT =
			"UPDATE district SET d_next_o_id = ? "
			+ "WHERE w_id = ? AND d_id = ?";
	private static final String SELECT_STOCK =
			"SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_? "
			+ "FROM stock "
			+ "WHERE w_id = ? AND i_id = ?";
	private static final String UPDATE_STOCK = 
			"UPDATE stock "
			+ "SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? "
			+ "WHERE w_id = ? AND i_id = ?";
	private static final String SELECT_ITEM =
			"SELECT i_price, i_name"
			+ "FROM item "
			+ "WHERE i_id = ?";
	private static final String SELECT_CUSTOMER =
			"SELECT c_last, c_credit, c_discount "
			+ "FROM customer "
			+ "WHERE w_id = ? AND d_id = ? AND c_id = ?";
	private static final String INSERT_ORDER =
			"INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, "
			+ "o_ol_cnt, o_all_local, o_entry_d) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";
	private static final String INSERT_ORDERLINE = 
			"INSERT INTO orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,"
			+ "ol_delivery_id, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
	
	private PreparedStatement warehouseSelect;
	private PreparedStatement districtSelect;
	private PreparedStatement customerSelect;
	private PreparedStatement stockSelect;
	private PreparedStatement itemSelect;
	private PreparedStatement districtUpdate;
	private PreparedStatement stockUpdate;
	private PreparedStatement orderInsert;
	private PreparedStatement orderLineInsert;
	private Row targetWarehouse;
	private Row targetDistrict;
	private Row targetCustomer;
	private Row targetStock;
	private Row targetItem;
	private Session session;
	private float total_amount = 0;
	private Date o_entry_id;
	
	public NewOrder(CassandraConnect connect) {
		this.session = connect.getSession();
		this.warehouseSelect = session.prepare(SELECT_WAREHOUSE);
		this.districtSelect = session.prepare(SELECT_DISTRICT);
		this.districtUpdate = session.prepare(UPDATE_DISTRICT);
		this.customerSelect = session.prepare(SELECT_CUSTOMER);
		this.stockSelect = session.prepare(SELECT_STOCK);
		this.stockUpdate = session.prepare(UPDATE_STOCK);
		this.itemSelect = session.prepare(SELECT_ITEM);
		this.orderInsert = session.prepare(INSERT_ORDER);
		this.orderLineInsert = session.prepare(INSERT_ORDERLINE);
	}
	
	public void processNewOrder(final int w_id, final int d_id, final int c_id, 
			final int num_items, final int[] item_number, 
			final int[] supplier_warehouse, final int[] quantity) {
		selectWarehouse(w_id);
		selectDistrict(w_id, d_id);
		selectCustomer(w_id, d_id, c_id);
		updateDistrict(w_id, d_id);
		insertOrder(w_id, d_id, c_id, num_items, supplier_warehouse);
		insertOrderLines(w_id, d_id, num_items, item_number, supplier_warehouse, quantity);
		computeTotal();
		outputResults(num_items);
	}
	
	private void outputResults(int num_items) {
		System.out.println(String.format(MESSAGE_CUSTOMER, 
				targetCustomer.getInt("c_w_id"),
				targetCustomer.getInt("c_d_id"),
				targetCustomer.getInt("c_id"),
				targetCustomer.getString("c_last"),
				targetCustomer.getString("c_credit"),
				targetCustomer.getFloat("c_discount")));
				
		System.out.println(String.format(MESSAGE_WAREHOUSE, 
				targetWarehouse.getFloat("w_tax")));
				
		System.out.println(String.format(MESSAGE_DISTRICT, 
				targetDistrict.getFloat("d_tax")));
		
		System.out.println(String.format(MESSAGE_ORDER, 
				targetDistrict.getInt("d_next_o_id") - 1,
				o_entry_id));
										
		System.out.println(String.format(MESSAGE_NUM_ITEMS,
				num_items));
												
		System.out.println(String.format(MESSAGE_TOTAL_AMOUNT,
				total_amount));
	}
	
	private void selectWarehouse(final int w_id) {
		ResultSet resultSet = session.execute(warehouseSelect.bind(w_id));
		List<Row> warehouses = resultSet.all();
		
		if(!warehouses.isEmpty()) {
			targetWarehouse = warehouses.get(0);
		}
	}
	
	private void selectDistrict(final int w_id, final int d_id) {
		ResultSet resultSet = session.execute(districtSelect.bind(w_id, d_id));
		List<Row> districts = resultSet.all();
		
		if(!districts.isEmpty()) {
			targetDistrict = districts.get(0);
		}
	}
	
	private void updateDistrict(final int w_id, final int d_id) {
		int d_next_o_id = targetDistrict.getInt("d_next_o_id") + 1;
		session.execute(districtUpdate.bind(d_next_o_id, w_id, d_id));
	}
	
	private void selectCustomer(final int w_id, final int d_id, final int c_id) {
		ResultSet resultSet = session.execute(customerSelect.bind(w_id, d_id, c_id));
		List<Row> customers = resultSet.all();
		
		if(!customers.isEmpty()) {
			targetCustomer = customers.get(0);
		}
	}
	
	private void selectItem(final int i_id) {
		ResultSet resultSet = session.execute(itemSelect.bind(i_id));
		List<Row> items = resultSet.all();
		
		if(!items.isEmpty()) {
			targetItem = items.get(0);
		}
	}
	
	private void selectStock(final int w_id, final int d_id, final int i_id, final int warehouse, final int quantity) {	
		ResultSet resultSet = session.execute(stockSelect.bind(d_id, w_id, i_id));
		List<Row> stocks = resultSet.all();
		
		if(!stocks.isEmpty()) {
			targetStock = stocks.get(0);
		}
	}
	
	private void updateStock(final int w_id, final int d_id, final int i_id, final int warehouse, final int quantity) {	
		int s_quantity = targetStock.getInt("s_quantity") - quantity;
		if(s_quantity < 10) {
			s_quantity = s_quantity + 100;
		}
		
		float s_ytd = targetStock.getFloat("s_ytd") + quantity;
		int s_order_cnt = targetStock.getInt("s_order_cnt") + 1;
		
		int s_remote_cnt = 0;
		if(warehouse != w_id) {
			s_remote_cnt = 1;
		}
		
		session.execute(stockUpdate.bind(s_quantity, s_ytd, s_order_cnt, s_remote_cnt, warehouse, i_id));
	}
	
	private void insertOrder(final int w_id, final int d_id, 
			final int c_id, final int num_items, final int[] supplier_warehouse) {
		
		o_entry_id = new Date();
		int o_id = targetDistrict.getInt("d_next_o_id") - 1;
		int o_all_local = 1;
		
		for (int id : supplier_warehouse) {
			if (w_id != id) {
				o_all_local = 0;
				break;
			}
		}
		
		session.execute(orderInsert.bind(w_id, d_id, o_id, c_id, null,
				num_items, o_all_local, o_entry_id));
	}
	
	private void insertOrderLines(final int w_id, final int d_id,  
			final int num_items, final int[] item_number, 
			final int[] supplier_warehouse, final int[] quantity) {
		
		int o_id = targetDistrict.getInt("d_next_o_id") - 1;
		
		for(int i = 0; i < num_items; i++) {
			selectStock(w_id, d_id, item_number[i], supplier_warehouse[i], quantity[i]);
			updateStock(w_id, d_id, item_number[i], supplier_warehouse[i], quantity[i]);
			selectItem(item_number[i]);	
			float item_amount = quantity[i] * targetItem.getFloat("item_price");		
			total_amount = total_amount + item_amount;
			String d_dist_id = String.format("d_dist_%1$s", d_id);
			session.execute(orderLineInsert.bind(w_id, d_id, o_id, i, 
					null, item_amount, supplier_warehouse[i], quantity[i], 
					targetStock.getString(d_dist_id)));
			
			System.out.println(String.format(MESSAGE_ORDER_ITEM, 
					item_number[i],
					targetItem.getString("item_name"),
					supplier_warehouse[i],
					quantity[i],
					item_amount,
					targetStock.getFloat("s_quantity")));
		}
	}
	
	private void computeTotal() {
		float w_tax = targetWarehouse.getFloat("w_tax");
		float d_tax = targetDistrict.getFloat("d_tax");
		float c_discount = targetCustomer.getFloat("c_discount");
		total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount);
	}
}