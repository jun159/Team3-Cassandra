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
	
	
	//=============================================================================
	// Constructor for StockLevel
	//=============================================================================
	public StockLevel(int w_id, int d_id, int stockThreshold, int numOfLastOrder) {
		w_id = this.w_id;
		d_id = this.d_id;
		stockThreshold = this.stockThreshold;
		numOfLastOrder = this.numOfLastOrder;
	}
	
}
