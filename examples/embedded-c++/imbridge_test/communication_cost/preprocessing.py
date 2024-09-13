import duckdb

con_pf = duckdb.connect(
    "/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_raven.db")

con_uc = duckdb.connect(
    f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_tpcx_ai_sf10.db")

result_path = "/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/communication_cost"

# cols = 28
pf1 = """
DROP TABLE IF EXISTS pf1_used_data;
CREATE TABLE pf1_used_data AS
SELECT prop_location_score1, prop_location_score2, prop_log_historical_price, price_usd,
                           orig_destination_distance, prop_review_score, avg_bookings_usd, stdev_bookings_usd,
                           position, prop_country_id, prop_starrating, prop_brand_bool, count_clicks, count_bookings,
                           year, month, weekofyear, time, site_id, visitor_location_country_id, srch_destination_id,
                           srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count,
                           srch_room_count, srch_saturday_night_bool, random_bool
FROM Expedia_S_listings_extension JOIN Expedia_R1_hotels ON Expedia_S_listings_extension.prop_id = Expedia_R1_hotels.prop_id 
JOIN Expedia_R2_searches ON Expedia_S_listings_extension.srch_id = Expedia_R2_searches.srch_id WHERE prop_location_score1 > 1 and prop_location_score2 > 0.1 
and prop_log_historical_price > 4 and count_bookings > 5 
and srch_booking_window > 10 and srch_length_of_stay > 1;
"""
con_pf.execute(pf1)

pf1_save = f"COPY pf1_used_data TO '{result_path}/pf1_used_data.csv' (HEADER TRUE, FORMAT CSV)"
con_pf.execute(pf1_save)

print(con_pf.sql("select * from pf1_used_data"))

# externel output

sql1 = f"""
COPY Expedia_S_listings_extension TO '{result_path}/Expedia_S_listings_extension.csv' (HEADER TRUE, FORMAT CSV);
COPY Expedia_R1_hotels  TO '{result_path}/Expedia_R1_hotels.csv' (HEADER TRUE, FORMAT CSV);
COPY Expedia_R2_searches  TO '{result_path}/Expedia_R2_searches.csv' (HEADER TRUE, FORMAT CSV);
"""
con_pf.execute(sql1)


# cols = 29
pf7 = """
DROP TABLE IF EXISTS pf7_used_data;
CREATE TABLE pf7_used_data AS
SELECT V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15,
 V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount FROM Credit_Card_extension 
 WHERE V1 > 1 AND V2 < 0.27 AND V3 > 0.3;
"""

con_pf.execute(pf7)

pf7_save = f"COPY pf7_used_data TO '{result_path}/pf7_used_data.csv' (HEADER TRUE, FORMAT CSV)"
con_pf.execute(pf7_save)

print(con_pf.sql("select * from pf7_used_data"))

# cols = 2
uc10 = """
DROP TABLE IF EXISTS uc10_used_data;
CREATE TABLE uc10_used_data AS
select amount_norm, business_hour_norm
from (select transactionID, amount/transaction_limit amount_norm, hour(strptime(time, '%Y-%m-%dT%H:%M'))/23 business_hour_norm 
from Financial_Account join Financial_Transactions on Financial_Account.fa_customer_sk = Financial_Transactions.senderID);
"""

uc10_save = f"COPY uc10_used_data TO '{result_path}/uc10_used_data.csv' (HEADER TRUE, FORMAT CSV)"
con_uc.execute(uc10_save)

con_uc.execute(uc10)
print(con_uc.sql("select * from uc10_used_data"))