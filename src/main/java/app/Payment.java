package app;

import java.util.List;

import com.datastax.driver.core.*;

public class Payment {
	
	private static final String MESSAGE_WAREHOUSE = "Warehouse address: Street(%1$s %1$s) City(%1$s) State(%1$s) Zip(%1$s)";
	private static final String MESSAGE_DISTRICT = "District address: Street(%1$s %1$s) City(%1$s) State(%1$s) Zip(%1$s)";
	private static final String MESSAGE_CUSTOMER = "Customer information: ID(%1$s, %1$s, %1$s), Name(%1$s, %1$s, %1$s), "
			+ "Address(%1$s, %1$s, %1$s, %1$s, %1$s), Phone(%1$s), Since(%1$s), Credits(%1$s, %1$s, %1$s, %1$s)";
	private static final String MESSAGE_PAYMENT = "Payment amount: %1$s";
	
	private static final String WAREHOUSE_SELECT = 
			"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd "
			+ "FROM warehouse "
			+ "WHERE w_id = ?;";
	private static final String WAREHOUSE_UPDATE = 
			"UPDATE warehouse "
			+ "SET w_ytd = ? "
			+ "WHERE w_id = ?;";
	private static final String DISTRICT_SELECT = 
			"SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd "
			+ "FROM district "
			+ "WHERE d_w_id = ? AND d_id = ?;";
	private static final String DISTRICT_UPDATE = 
			"UPDATE district "
			+ "SET d_ytd = ? "
			+ "WHERE d_w_id = ? AND d_id = ?;";
	private static final String CUSTOMER_SELECT = 
			"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
			+ "c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
			+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt "
			+ "FROM customer "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
	private static final String CUSTOMER_UPDATE = 
			"UPDATE customer "
			+ "SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
	
	private PreparedStatement warehouseSelect;
	private PreparedStatement warehouseUpdate;
	private PreparedStatement districtSelect;
	private PreparedStatement districtUpdate;
	private PreparedStatement customerSelect;
	private PreparedStatement customerUpdate;
	private Row targetWarehouse;
	private Row targetDistrict;
	private Row targetCustomer;
	private Session session;
	
	public Payment(CassandraConnect connect) {
		this.session = connect.getSession();
		this.warehouseSelect = session.prepare(WAREHOUSE_SELECT);
		this.warehouseUpdate = session.prepare(WAREHOUSE_UPDATE);
		this.districtSelect = session.prepare(DISTRICT_SELECT);
		this.districtUpdate = session.prepare(DISTRICT_UPDATE);
		this.customerSelect = session.prepare(CUSTOMER_SELECT);
		this.customerUpdate = session.prepare(CUSTOMER_UPDATE);
	}
	
	public void processPayment(final int c_w_id, final int c_d_id, 
			final int c_id, final float payment) {
		updateWarehouse(c_w_id, payment);
		updateDistrict(c_w_id, c_d_id, payment);
		updateCustomer(c_w_id, c_d_id, c_id, payment);
		outputResults(payment);
	}
	
	private void outputResults(float payment) {
		System.out.println(String.format(MESSAGE_CUSTOMER, 
				targetCustomer.getString("c_w_id"),
				targetCustomer.getString("c_d_id"),
				targetCustomer.getString("c_id"),
				
				targetCustomer.getString("c_first"),
				targetCustomer.getString("c_middle"),
				targetCustomer.getString("c_last"),
				
				targetCustomer.getString("c_street_1"),
				targetCustomer.getString("c_street_2"),
				targetCustomer.getString("c_city"),
				targetCustomer.getString("c_state"),
				targetCustomer.getString("c_zip"),
				
				targetCustomer.getString("c_phone"),
				targetCustomer.getString("c_since"),
				
				targetCustomer.getString("c_credit"),
				targetCustomer.getString("c_credit_lim"),
				targetCustomer.getString("c_discount"),
				targetCustomer.getString("c_balance")));
		
		System.out.println(String.format(MESSAGE_WAREHOUSE, 
				targetWarehouse.getString("w_street_1"),
				targetWarehouse.getString("w_street_2"),
				targetWarehouse.getString("w_city"),
				targetWarehouse.getString("w_state"),
				targetWarehouse.getString("w_zip")));
		
		System.out.println(String.format(MESSAGE_DISTRICT, 
				targetDistrict.getString("d_street_1"),
				targetDistrict.getString("d_street_2"),
				targetDistrict.getString("d_city"),
				targetDistrict.getString("d_state"),
				targetDistrict.getString("d_zip")));
		
		System.out.println(String.format(MESSAGE_PAYMENT, payment));
	}
	
	private void updateWarehouse(final int c_w_id, final float payment) {
		ResultSet resultSet = session.execute(warehouseSelect.bind(c_w_id));
		List<Row> warehouses = resultSet.all();
		
		if(!warehouses.isEmpty()) {
			targetWarehouse = warehouses.get(0);
			float w_ytd = targetWarehouse.getFloat("w_ytd") + payment;
			session.execute(warehouseUpdate.bind(w_ytd, c_w_id));	
		}
	}
	
	private void updateDistrict(final int c_w_id, final int c_d_id, final float payment) {
		ResultSet resultSet = session.execute(districtSelect.bind(c_w_id, c_d_id));
		List<Row> districts = resultSet.all();
		
		if(!districts.isEmpty()) {
			Row targetDistrict = districts.get(0);
			float d_ytd = targetDistrict.getFloat("d_ytd") + payment;
			session.execute(districtUpdate.bind(d_ytd, c_w_id, c_d_id));
		}
	}
	
	private void updateCustomer(final int c_w_id, final int c_d_id, 
			final int c_id, final float payment) {
		ResultSet resultSet = session.execute(customerSelect.bind(c_w_id, c_d_id, c_id));
		List<Row> customers = resultSet.all();
		
		if(!customers.isEmpty()) {
			Row targetCustomer = customers.get(0);
			float c_balance = targetCustomer.getFloat("c_balance") - payment;
			float c_ytd_payment = targetCustomer.getFloat("c_ytd_payment") + payment;
			int c_payment_cnt = targetCustomer.getInt("c_payment_cnt") + 1;
			session.execute(customerUpdate.bind(c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id));
		}
	}
}