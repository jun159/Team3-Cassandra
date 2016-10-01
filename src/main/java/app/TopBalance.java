/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 * Transaction: Finds the top-10 customers ranked in descending order of their outstanding balance payments.
 */

package app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.datastax.driver.core.GettableByIndexData;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TopBalance {
	private static final String MESSAGE_CUSTOMER_NAME = "Name of Customer: %s\n";
	private static final String MESSAGE_CUSTOMER_BALANCE = "Customer Balance: %2f\n";
	private static final String MESSAGE_WAREHOUSE_NAME_OF_CUSTOMER = "Warehouse Name of Customer: %s\n";
	private static final String MESSAGE_DISTRICT_NAME_OF_CUSTOMER = "District Name of Customer: %s\n";

	//====================================================================================
	// CQL Queries for TopBalance transaction
	//====================================================================================
	private static final String CUSTOMER_SELECT = 
	          				  "SELECT c_balance, c_w_id, c_d_id, c_first, c_middle, c_last "
	          				+ "FROM team3.customer_by_balance "
	          				+ "WHERE c_w_id = ? "
	          				+ "LIMIT 10 ; ";
	
	private static final String WAREHOUSE_NAME_SELECT = 
							  "SELECT w_name "
							+ "FROM warehouse "
							+ "WHERE w_id = ? "
							+ "ALLOW FILTERING;";
	
	private static final String DISTRICT_NAME_SELECT = 
							  "SELECT d_name "
							+ "FROM district "
							+ "WHERE d_id = ? "
							+ "ALLOW FILTERING;";
	
	//====================================================================================
	// Preparing for session
	//====================================================================================

	private Session session;
	
	private PreparedStatement topBalance_Select;
	private PreparedStatement warehouseName_Select;
	private PreparedStatement districtName_Select;
	private Row targetDistrict;
	private Row targetWarehouse;
	
	public TopBalance(CassandraConnect connect) {
		this.session = connect.getSession();
		this.topBalance_Select = session.prepare(CUSTOMER_SELECT);
		this.warehouseName_Select = session.prepare(WAREHOUSE_NAME_SELECT);
		this.districtName_Select = session.prepare(DISTRICT_NAME_SELECT);
	}

	//====================================================================================
	// Processing Top Balance transaction
	//====================================================================================

	public void processTopBalance() {
		ArrayList<Row> result = new ArrayList<Row>();
		
		for(int i = 1; i < 9; i++) {
			ResultSet resultSet = session.execute(topBalance_Select.bind(i));
			ArrayList<Row> set = (ArrayList<Row>) resultSet.all();
			result.addAll(set);
		}
		
		Collections.sort(result, new Comparator<Row>() {
            public int compare(Row o1, Row o2) {
                return (int) (o2.getDouble("c_balance") - o1.getDouble("c_balance"));
            }
        });
		
		for(int i = 0; i < 10; i++) {
			printCustomerName(result.get(i).getString("c_first"));
			printCustomerBalance(result.get(i).getDouble("c_balance"));
			printWarehouseNameOfCustomer(getWarehouseName(result.get(i).getInt("c_w_id")));
			printDistrictNameOfCustomer(getDistrictName(result.get(i).getInt("c_d_id")));
			System.out.println();
		}
		
		System.out.println("\n==========================End of Transaction============================\n");
	}
	
	public String getWarehouseName(int warehouse_ID) {

		ResultSet resultSet = session.execute(warehouseName_Select.bind(warehouse_ID));
		List<Row> warehouse = resultSet.all();

		if(!warehouse.isEmpty()) {
			targetWarehouse = warehouse.get(0);
			return targetWarehouse.getString("w_name");
		}
		
		return null;
	}
	
	public String getDistrictName(int district_ID) {

		ResultSet resultSet = session.execute(districtName_Select.bind(district_ID));
		List<Row> district = resultSet.all();

		if(!district.isEmpty()) {
			targetDistrict = district.get(0);
			return targetDistrict.getString("d_name");
		}

		return null;
	}

	private void printCustomerName(String customerName) {
		System.out.printf(MESSAGE_CUSTOMER_NAME, customerName);
	}

	private void printCustomerBalance(double d) {
		System.out.printf(MESSAGE_CUSTOMER_BALANCE, d);
	}

	private void printWarehouseNameOfCustomer(String warehouseName) {
		System.out.printf(MESSAGE_WAREHOUSE_NAME_OF_CUSTOMER, warehouseName);
	}

	private void printDistrictNameOfCustomer(String districtName) {
		System.out.printf(MESSAGE_DISTRICT_NAME_OF_CUSTOMER, districtName);
	}
}
