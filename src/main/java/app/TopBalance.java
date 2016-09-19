/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 * Transaction: Finds the top-10 customers ranked in descending order of their outstanding balance payments.
 */

package app;

public class TopBalance {
	private String MESSAGE_CUSTOMER_NAME = "Name of Customer: %s";
	private String MESSAGE_CUSTOMER_BALANCE = "Customer Balance: %s";
	private String MESSAGE_WAREHOUSE_NAME_OF_CUSTOMER = "Warehouse Name of Customer: %s";
	private String MESSAGE_DISTRICT_NAME_OF_CUSTOMER = "District Name of Customer: %s";
	
	
	
	
	
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
