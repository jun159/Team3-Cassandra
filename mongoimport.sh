./mongoimport -d team3 -c warehouse --type csv --file warehouse.csv --fields w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd 

./mongoimport -d team3 -c district --type csv --file district.csv --fields d_w_id,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id

./mongoimport -d team3 -c customer --type csv --file customer.csv --fields c_w_id,c_d_id,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data

./mongoimport -d team3 -c orders --type csv --file order.csv --fields o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d

./mongoimport -d team3 -c item --type csv --file item.csv --fields i_id,i_name,i_price,i_im_id,i_data

./mongoimport -d team3 -c orderline --type csv --file order-line.csv --fields ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info

./mongoimport -d team3 -c stock --type csv --file stock.csv --fields s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data
