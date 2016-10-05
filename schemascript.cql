DROP TABLE IF EXISTS team3.Warehouse;
DROP TABLE IF EXISTS team3.District;
DROP TABLE IF EXISTS team3.Customer;
DROP TABLE IF EXISTS team3.Orders;
DROP TABLE IF EXISTS team3.OrderLine;
DROP TABLE IF EXISTS team3.StockItem;


USE team3;

CREATE TABLE Warehouse (
	W_ID int,
	W_NAME varchar,
	W_STREET_1 varchar,
	W_STREET_2 varchar,
	W_CITY varchar,
	W_STATE varchar,
	W_ZIP varchar,
	W_TAX double,
	W_YTD double,
	PRIMARY KEY (W_ID)
);

CREATE TABLE District (
	D_W_ID int,
	D_ID int,
	D_NAME varchar,
	D_STREET_1 varchar,
	D_STREET_2 varchar,
	D_CITY varchar,
	D_STATE varchar,
	D_ZIP varchar,
	D_TAX double,
	D_YTD double,
	D_NEXT_O_ID int,
	PRIMARY KEY (D_W_ID, D_ID)
);

CREATE TABLE Customer (
	C_W_ID int,
	C_D_ID int,
	C_ID int,
	C_FIRST varchar,
	C_MIDDLE varchar,
	C_LAST varchar,
	C_STREET_1 varchar,
	C_STREET_2 varchar,
	C_CITY varchar,
	C_STATE varchar,
	C_ZIP varchar,
	C_PHONE varchar,
	C_SINCE timestamp,
	C_CREDIT varchar,
	C_CREDIT_LIM double,
	C_DISCOUNT double,
	C_BALANCE double,
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	C_DATA varchar,
	PRIMARY KEY((C_W_ID, C_D_ID), C_ID) 
);

DROP MATERIALIZED VIEW IF EXISTS team3.customer_by_balance;

CREATE MATERIALIZED VIEW IF NOT EXISTS team3.customer_by_balance AS
SELECT c_balance, c_w_id, c_d_id, c_id, c_first, c_middle, c_last
	FROM customer
	WHERE c_w_id IS NOT NULL 
	AND c_balance IS NOT NULL
	AND c_d_id IS NOT NULL
	AND c_id IS NOT NULL
	PRIMARY KEY (c_w_id, c_balance, c_d_id, c_id)
WITH CLUSTERING ORDER BY (c_balance DESC);

CREATE TABLE Orders (
	O_W_ID int,
	O_D_ID int,
	O_ID int,
	O_C_ID int,
	O_CARRIER_ID int,
	O_OL_CNT double,
	O_ALL_LOCAL double,
	O_ENTRY_D timestamp,
	PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)
) WITH CLUSTERING ORDER BY (O_ID DESC);

CREATE TABLE OrderLine (
	OL_W_ID int,
	OL_D_ID int,
	OL_O_ID int,
	OL_NUMBER int,
	OL_I_ID int,
	OL_DELIVERY_D timestamp,
	OL_AMOUNT double,
	OL_SUPPLY_W_ID int,
	OL_QUANTITY double,
	OL_DIST_INFO varchar,
	PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_NUMBER)
);

CREATE INDEX ON OrderLine (OL_O_ID);
CREATE INDEX ON OrderLine (OL_QUANTITY);

CREATE TABLE team3.StockItem ( 
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY double,
	S_YTD double,
	S_ORDER_CNT int,
	S_REMOTE_CNT int,
	S_DIST_01 varchar,
	S_DIST_02 varchar,
	S_DIST_03 varchar,
	S_DIST_04 varchar,
	S_DIST_05 varchar,
	S_DIST_06 varchar,
	S_DIST_07 varchar,
	S_DIST_08 varchar,
	S_DIST_09 varchar,
	S_DIST_10 varchar,
	S_DATA varchar,
	I_NAME varchar,
	I_PRICE double,
	I_IM_ID int,
	I_DATA varchar,
	PRIMARY KEY ((S_W_ID), S_I_ID)
);

copy team3.warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) from '~/Team3-Cassandra/data/D8-data/warehouse.csv';

copy team3.district (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) from '~/Team3-Cassandra/data/D8-data/district.csv';

copy team3.customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) from '~/Team3-Cassandra/data/D8-data/customer.csv';

copy team3.orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) from '~/Team3-Cassandra/data/D8-data/order.csv' WITH NULL = 'null';

copy team3.orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) from '~/Team3-Cassandra/data/D8-data/order-line.csv' WITH NULL = 'null';
