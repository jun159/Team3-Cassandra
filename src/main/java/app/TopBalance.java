/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 * Transaction: Finds the top-10 customers ranked in descending order of their outstanding balance payments.
 */

package app;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class TopBalance {
	private static final String MESSAGE_CUSTOMER_NAME = "Name of Customer: %s";
	private static final String MESSAGE_CUSTOMER_BALANCE = "Customer Balance: %2f";
	private static final String MESSAGE_WAREHOUSE_NAME_OF_CUSTOMER = "Warehouse Name of Customer: %s";
	private static final String MESSAGE_DISTRICT_NAME_OF_CUSTOMER = "District Name of Customer: %s";

	//====================================================================================
	// CQL Queries for TopBalance transaction
	//====================================================================================
	private static final String CUSTOMER_SELECT = 
	          "SELECT c_balance, c_w_id, c_d_id, c_first, c_middle, c_last"
			+ "FROM customer;";
	
	
	//====================================================================================
	// Preparing for session
	//====================================================================================

	private Session session;

	public TopBalance(CassandraConnect connect) {
		this.session = connect.getSession();
	}

	//====================================================================================
	// Processing Top Balance transaction
	//====================================================================================

	public void processTopBalance() {
		
	}

	private void printCustomerName(String customerName) {
		System.out.printf(MESSAGE_CUSTOMER_NAME, customerName);
	}

	private void printCustomerBalance(float balance) {
		System.out.printf(MESSAGE_CUSTOMER_BALANCE, balance);
	}

	private void printWarehouseNameOfCustomer(String warehouseName) {
		System.out.printf(MESSAGE_WAREHOUSE_NAME_OF_CUSTOMER, warehouseName);
	}

	private void printDistrictNameOfCustomer(String districtName) {
		System.out.printf(MESSAGE_DISTRICT_NAME_OF_CUSTOMER, districtName);
	}
}
