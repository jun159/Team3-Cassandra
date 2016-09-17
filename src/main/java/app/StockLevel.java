/* Author: Lin Jiahao
 * ID: A0108235M
 * Team: 3
 */

package app;

public class StockLevel {
	private int w_id;
	private int d_id;
	private int stockThreshold;
	private int numOfLastOrder;
	private int numOfItemBelowThreshold;
	
	//=============================================================================
	// Constructor for StockLevel
	//=============================================================================
	public StockLevel(int w_id, int d_id, int stockThreshold, int numOfLastOrder) {
		w_id = this.w_id;
		d_id = this.d_id;
		stockThreshold = this.stockThreshold;
		numOfLastOrder = this.numOfLastOrder;
	}
	
	public int getNextAvailableOrderNum() {
		int nextAvailableNum = 0;
		
		// CQL: Retrieve next available number from DB
		
		return nextAvailableNum;
	}
	
	public void countItemBelowThreshold() {
		numOfItemBelowThreshold = 0;
		
		// CQL: Retrieve set of items from last L orders for district (W ID,D ID)
		
	}
	
	public void printItemBelowThreshold() {
		System.out.println(numOfItemBelowThreshold);
	}
	
}
