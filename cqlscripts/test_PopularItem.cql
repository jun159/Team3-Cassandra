// Purpose: Find the most popular item(s) in each of the last L orders at a specified warehouse district
// Transaction Example: I,1,1,50

// 1. Get the next available order number d_next_o_id for district (w_id, d_id)
SELECT d_next_o_id 
FROM team3.district 
WHERE d_w_id = 1 and d_id = 1;

// 2. Get the set of last L orders from orders table
SELECT o_id 
FROM team3.orders 
WHERE o_d_id = 1 
AND o_w_id = 1
AND o_id >= 15286 and o_id < 15336;

// 3. Get max ol_quantity in this order
// Output = 9
SELECT  max(ol_quantity) as max_quantity
FROM team3.orderline
WHERE ol_o_id = 15290;

// 4. Find popular item(s) in this order
SELECT ol_i_id, ol_quantity
FROM team3.orderline
WHERE ol_o_id = 15302 and ol_quantity = 10
ALLOW FILTERING;