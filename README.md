# Performance
import pandas as pd 
import pymysql
import numpy as np
import datetime as dt
conn = pymysql.connections.Connection()
query1 = """select 'Won' as Task,l.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,
sum(case when  date(vb.created_at) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(vb.created_at) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(vb.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(vb.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) and 
        date(vb.created_at) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as LM

from vehicle_boughts vb
left join leads l on l.id=lead_id
left join stores s on s.id=l.store_id
left join cities c on c.id=s.city_id

where date(vb.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)
group by l.business_type,3

UNION

select 'Stock In' as Task,l.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(a.l) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(a.l) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(a.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(a.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)

and   date(a.l) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as LM
from (select lead_id,min(entry_date) as l
from vehicle_stocks vs
group by vs.lead_id
having  min(date(vs.entry_date))>=((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)) a
left join leads l on l.id=a.lead_id
left join stores s on s.id=l.store_id
left join cities c on c.id=s.city_id
group by l.business_type,3;"""
Automation1 = pd.read_sql_query(query1,conn)
Automation1.head()
query2 = """
select 'Fresh_Inspection' as Task,l.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(a.l) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(a.l) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(a.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(a.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) 
and date(a.l) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) 
then 1 else 0 end) as LM

from (select v.lead_id,min(r.created_at) as l
from report_short_terms r
left join vtbs v on v.id=r.vtb_id
group by v.lead_id
having  min(date(r.created_at))>=((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)) a
left join leads l on l.id=a.lead_id
left join stores s on s.id=l.store_id
left join cities c on c.id=s.city_id
group by l.business_type,3;

"""
Automation2 = pd.read_sql_query(query2,conn)
Automation2.head()
query3 = """
select 'Stock_out' as Task,c.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(c.l) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(c.l) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) 
and date(c.l) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as LM

from 
(select * from 
(select  concat('#',l.ref_code) as refcode,l.business_type,

(select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ) as IM_deduction_amount,
vt.final_price,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1) as sold_amount,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1)-(vt.final_price+IFNULL((select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ),0)) as Rake,

(select date(entry_date) from vehicle_stocks
where lead_id=vs.lead_id and stock_type='stock_out'
order by id desc limit 1 ) as l,

(select 
case
when dealership_id is null and stock_type='stock_out' and (transit_status='in_transit' or destination_yard_id !=0)  then 'In-transit' 
when stock_type='stock_out' and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is not null
then 'Final_stock_out'
when stock_type='stock_out' and dealership_id is not null and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is  null
then 'Final_stock_out_without_payment' 
when dealership_id is null and stock_type='stock_out' then 'In-transit' 
else 'Stock_in' end  as stockout_type from vehicle_stocks
where lead_id=vs.lead_id 
order by id desc limit 1 ) as finalstockout_type,


ifnull ((select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join cities ctt on ctt.id=sss.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 ),

(select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join stores st on st.id=sss.store_id
left join cities ctt on ctt.id=st.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 )
) as name

from vehicle_stocks vs
left join leads l on l.id=vs.lead_id
left join stocks s on s.id=vs.stock_id 
left join stores ss on ss.id=l.store_id
left join cities ct on ct.id=l.city_id

left join vtbs vt on vt.lead_id=vs.lead_id
left join vehicle_boughts vb on vb.lead_id=vs.lead_id
left join dealerships dd on dd.id=vb.highest_bid_dealer_id
left join vehicle_solds so on so.lead_id=vs.lead_id
left join dealerships d on d.id=so.dealership_id

group by l.ref_code,l.registration_num) a 

where a.l>= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) and a.l<=current_date
and finalstockout_type in ('Final_stock_out','Final_stock_out_without_payment')) c

group by c.business_type,3;

"""

Automation3 = pd.read_sql_query(query3,conn)
Automation3.head()
query4 = """
select 'Stock_out_Rake' as Task,c.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(c.l) = CURRENT_DATE then Rake else 0 end) as Today,
sum(case when  date(c.l) = (CURRENT_DATE - INTERVAL 1 day) then Rake else 0 end) as Yesterday,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then Rake else 0 end) as MTD,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)
and date(c.l) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month)
then Rake else 0 end) as LM

from 
(select * from 
(select  concat('#',l.ref_code) as refcode,l.business_type,

(select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ) as IM_deduction_amount,
vt.final_price,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1) as sold_amount,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1)-(vt.final_price+IFNULL((select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ),0)) as Rake,

(select date(entry_date) from vehicle_stocks
where lead_id=vs.lead_id and stock_type='stock_out'
order by id desc limit 1 ) as l,

(select 
case
when dealership_id is null and stock_type='stock_out' and (transit_status='in_transit' or destination_yard_id !=0)  then 'In-transit' 
when stock_type='stock_out' and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is not null
then 'Final_stock_out'
when stock_type='stock_out' and dealership_id is not null and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is  null
then 'Final_stock_out_without_payment' 
when dealership_id is null and stock_type='stock_out' then 'In-transit' 
else 'Stock_in' end  as stockout_type from vehicle_stocks
where lead_id=vs.lead_id 
order by id desc limit 1 ) as finalstockout_type,


ifnull ((select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join cities ctt on ctt.id=sss.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 ),

(select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join stores st on st.id=sss.store_id
left join cities ctt on ctt.id=st.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 )
) as name

from vehicle_stocks vs
left join leads l on l.id=vs.lead_id
left join stocks s on s.id=vs.stock_id 
left join stores ss on ss.id=l.store_id
left join cities ct on ct.id=l.city_id

left join vtbs vt on vt.lead_id=vs.lead_id
left join vehicle_boughts vb on vb.lead_id=vs.lead_id
left join dealerships dd on dd.id=vb.highest_bid_dealer_id
left join vehicle_solds so on so.lead_id=vs.lead_id
left join dealerships d on d.id=so.dealership_id

group by l.ref_code,l.registration_num) a 

where a.l>= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) and a.l<=current_date
and finalstockout_type in ('Final_stock_out','Final_stock_out_without_payment')) c

group by c.business_type,3;
"""
Automation4 = pd.read_sql_query(query4,conn)
Automation4.head()
query5 ="""
select 'Stock_out_Dealer' as Task,c.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(c.l) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(c.l) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)
and  date(c.l) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as LM

from
(select * from 
(select  concat('#',l.ref_code) as refcode,l.business_type,

(select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ) as IM_deduction_amount,
vt.final_price,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1) as sold_amount,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1)-(vt.final_price+IFNULL((select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ),0)) as Rake,

(select date(entry_date) from vehicle_stocks
where lead_id=vs.lead_id and stock_type='stock_out'
order by id desc limit 1 ) as l,

(select 
case
when dealership_id is null and stock_type='stock_out' and (transit_status='in_transit' or destination_yard_id !=0)  then 'In-transit' 
when stock_type='stock_out' and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is not null
then 'Final_stock_out'
when stock_type='stock_out' and dealership_id is not null and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is  null
then 'Final_stock_out_without_payment' 
when dealership_id is null and stock_type='stock_out' then 'In-transit' 
else 'Stock_in' end  as stockout_type from vehicle_stocks
where lead_id=vs.lead_id 
order by id desc limit 1 ) as finalstockout_type,
ifnull ((select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join cities ctt on ctt.id=sss.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 ),

(select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join stores st on st.id=sss.store_id
left join cities ctt on ctt.id=st.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 )
) as name




from vehicle_stocks vs
left join leads l on l.id=vs.lead_id
left join stocks s on s.id=vs.stock_id 
left join stores ss on ss.id=l.store_id
left join cities ct on ct.id=l.city_id


left join vtbs vt on vt.lead_id=vs.lead_id
left join vehicle_boughts vb on vb.lead_id=vs.lead_id
left join dealerships dd on dd.id=vb.highest_bid_dealer_id
left join vehicle_solds so on so.lead_id=vs.lead_id
left join dealerships d on d.id=so.dealership_id
where d.id not in (14709,15363,15384,15326,14525,15367,1171,16031,14454,16751,15040,16377,15792,16762,16661,16764,14441,16766,16763,16768,16767,16872,16874,16873,16765,18102,18104,18103)
group by l.ref_code,l.registration_num) a 

where a.l>= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) and a.l<=current_date
and finalstockout_type in ('Final_stock_out','Final_stock_out_without_payment')) c

group by c.business_type,3;
"""
Automation4_dealer_stock_out = pd.read_sql_query(query5,conn)
query6= """
select 'Stock_out_TM/B2C    ' as Task,c.business_type as Business_type,
case
when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
when c.name in ('Kolkata') then 'WB'
when c.name in ('Bhubaneswar') then 'OD'
when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(c.l) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(c.l) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(c.l) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) 
AND date(c.l) <= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month)
then 1 else 0 end) as LM

from 
(


select * from 
(select  concat('#',l.ref_code) as refcode,l.business_type,

(select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ) as IM_deduction_amount,
vt.final_price,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1) as sold_amount,
(select amount from vehicle_solds where lead_id=vs.lead_id order by id desc limit 1)-(vt.final_price+IFNULL((select sum(negotiated_deduction_amount) from inspection_misses  where lead_id=l.id ),0)) as Rake,

(select date(entry_date) from vehicle_stocks
where lead_id=vs.lead_id and stock_type='stock_out'
order by id desc limit 1 ) as l,

(select 
case
when dealership_id is null and stock_type='stock_out' and (transit_status='in_transit' or destination_yard_id !=0)  then 'In-transit' 
when stock_type='stock_out' and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is not null
then 'Final_stock_out'
when stock_type='stock_out' and dealership_id is not null and 
(select sum(amount) from payment_dealers where status='verified' and lead_id=vs.lead_id) is  null
then 'Final_stock_out_without_payment' 
when dealership_id is null and stock_type='stock_out' then 'In-transit' 
else 'Stock_in' end  as stockout_type from vehicle_stocks
where lead_id=vs.lead_id 
order by id desc limit 1 ) as finalstockout_type,

ifnull ((select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join cities ctt on ctt.id=sss.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 ),

(select ctt.name 
from vehicle_stocks vss
left join stocks sss on sss.id=vss.stock_id 
left join stores st on st.id=sss.store_id
left join cities ctt on ctt.id=st.city_id
where lead_id=vs.lead_id and vss.stock_id>0
order by vss.entry_date desc limit 1 )
) as name


from vehicle_stocks vs
left join leads l on l.id=vs.lead_id
left join stocks s on s.id=vs.stock_id 
left join stores ss on ss.id=l.store_id
left join cities ct on ct.id=l.city_id

left join vtbs vt on vt.lead_id=vs.lead_id
left join vehicle_boughts vb on vb.lead_id=vs.lead_id
left join dealerships dd on dd.id=vb.highest_bid_dealer_id
left join vehicle_solds so on so.lead_id=vs.lead_id
left join dealerships d on d.id=so.dealership_id
where d.id in (14709,15363,15384,15326,14525,15367,1171,16031,14454,16751,15040,16377,15792,16762,16661,16764,14441,16766,16763,16768,16767,16872,16874,16873,16765,18102,18104,18103)
group by l.ref_code,l.registration_num) a 

where a.l>= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month) and a.l<=current_date
and finalstockout_type in ('Final_stock_out','Final_stock_out_without_payment')) c

group by c.business_type,3;
"""
Automation4_TM_stock_out = pd.read_sql_query(query6,conn)
Automation4_TM_stock_out.head()
query7="""
select 'B2C/TM Allocated' as Task,l.business_type as Business_type,
  case
    when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
    when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
    when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
    when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
    when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
    when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
    when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
    when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
    when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
    when c.name in ('Kolkata') then 'WB'
        when c.name in ('Bhubaneswar') then 'OD'
    when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
    when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(vk.created_at) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(vk.created_at) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(vk.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(vk.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)
AND date(vk.created_at) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month)
then 1 else 0 end) as LM

from (select lead_id,min(entry_date) as l
from vehicle_stocks vs

group by vs.lead_id
having  min(date(vs.entry_date))>='2021-01-01') a
left join leads l on l.id=a.lead_id
left join stores s on s.id=l.store_id
left join cities c on c.id=s.city_id
left join vehicle_solds vk on vk.lead_id=a.lead_id
where vk.dealership_id in (14709,15363,15384,15326,14525,15367,1171,16031,14454,16751,15040,16377,15792,16762,16661,16764,14441,16766,16763,16768,16767,16872,16874,16873,16765,18102,18104,18103 )
and (select 
case
when dealership_id is null and stock_type='stock_out' and (transit_status='in_transit' or destination_yard_id !=0)  then 'In-transit' 
  when stock_type='stock_out' and 
     (select sum(amount) from payment_dealers where status='verified' and lead_id=a.lead_id) is not null
  then 'Final_stock_out'
  when stock_type='stock_out' and dealership_id is not null and 
  (select sum(amount) from payment_dealers where status='verified' and lead_id=a.lead_id) is  null
  then 'Final_stock_out_without_payment' 
  when dealership_id is null and stock_type='stock_out' then 'In-transit' 
  else 'Stock_in' end  as stockout_type from vehicle_stocks
  where lead_id=a.lead_id 
  order by id desc limit 1 ) not in ('Final_stock_out_without_payment','Final_stock_out' )
group by l.business_type,3

"""



Automation4_TM_Allocation = pd.read_sql_query(query7,conn)
Automation4_TM_Allocation.head()

query8="""select 'B2C/TM Allocated' as Task,l.business_type as Business_type,
  case
    when c.name in ('Hyderabad','Visakhapatnam','Vijayawada','Madhapur','Rajahmundry','Secunderabad','Guntur','Karimnagar','Warangal') then 'APTS'
    when c.name in ('Vadodara','Ahmedabad','Surat','Gandhinagar','Rajkot','Rajkot','Anand','Junagadh','Vapi','Bharuch','Bhavnagar','Palanpur','Jamnagar') then 'GJ'
    when c.name in ('hisar','rohtak','Karnal','Bhiwani','Sonipat') then 'HR'
    when c.name in ('Bangalore','Cuttack','Mysore','Mangalore','Chitradurga','Sikandra','Chikmagalur','Gonikoppal') then 'KR'
    when c.name in ('Navi Mumbai','Mumbai','Pune','Panvel','Thane','Vasai','Nagpur','Pimpri chinchwad','Kharghar','Dombivli','Raigad','vashi','wagholi','kalyan','Tumkur','Aurangabad','Virar','Palghar') then 'MH' 
    when c.name in ('New Delhi','Ghaziabad','Faridabad','Noida','Gurugram','Kalpetta','Greater Noida','Gautam Buddha Nagar','Port Blair') then 'NCR' 
    when c.name in ('Chandigarh','Ludhiana','amritsar','mohali','Jalandhar','Zirakpur','Ambala') then 'PB'
    when c.name in ('Jaipur','Jodhpur','Ajmer','Ud aipur','alwar','Kota','Jhunjhunu','Bikaner','Bhilwara','Sawai Madhopur','Chittorgarh','Nagaur','Tonk','Sikar','Sri Ganganagar','Udaipur') then 'RJ' 
    when c.name in ('haldwani','Dehradun','Rampur Road') then 'UK'
    when c.name in ('Kolkata') then 'WB'
        when c.name in ('Bhubaneswar') then 'OD'
    when c.name in ('Lucknow','Agra','Varanasi','Kanpur','Meerut','Bareilly','allahabad','Kanpur Dehat','Aligarh','Muzaffarnagar','Barabanki','Azamgarh','Gorakhpur','Moradabad','Pratapgarh','Raebareli','Saharanpur (UP)','sultanpur') then 'UP' 
    when c.name in ('Chennai','Coimbatore') then 'TN' else 'Others' end  as region,

sum(case when  date(vk.created_at) = CURRENT_DATE then 1 else 0 end) as Today,
sum(case when  date(vk.created_at) = (CURRENT_DATE - INTERVAL 1 day) then 1 else 0 end) as Yesterday,
sum(case when  date(vk.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month) then 1 else 0 end) as MTD,
sum(case when  date(vk.created_at) >= ((current_date- interval (day(current_date)) day) + interval 1 day -interval 1 month)
AND date(vk.created_at) < ((current_date- interval (day(current_date)) day) + interval 1 day -interval 0 month)
then 1 else 0 end) as LM

   from (select lead_id,min(entry_date) as l
       from vehicle_stocks vs
       
   group by vs.lead_id
  having  min(date(vs.entry_date))>='2021-01-01') a
  left join leads l on l.id=a.lead_id
   left join stores s on s.id=l.store_id
 left join cities c on c.id=s.city_id
 left join vehicle_solds vk on vk.lead_id=a.lead_id
 where vk.dealership_id in (14709,15363,15384,15326,14525,15367,1171,16031,14454,16751,15040,16377,15792,16762,16661,16764,14441,16766,16763,16768,16767,16872,16874,16873,16765,18102,18104,18103 )
 and (select 
case
   when dealership_id is null and stock_type='stock_out' and (transit_status='in_transit' or destination_yard_id !=0)  then 'In-transit' 
  when stock_type='stock_out' and 
     (select sum(amount) from payment_dealers where status='verified' and lead_id=a.lead_id) is not null
  then 'Final_stock_out'
  when stock_type='stock_out' and dealership_id is not null and 
  (select sum(amount) from payment_dealers where status='verified' and lead_id=a.lead_id) is  null
  then 'Final_stock_out_without_payment' 
  when dealership_id is null and stock_type='stock_out' then 'In-transit' 
  else 'Stock_in' end  as stockout_type from vehicle_stocks
  where lead_id=a.lead_id 
  order by id desc limit 1 ) not in ('Final_stock_out_without_payment','Final_stock_out' )
group by l.business_type,3
 """
df2 = pd.read_sql_query(query8,conn)
df2.head()
df=pd.concat([Automation1,Automation2,Automation3,Automation4,Automation4_dealer_stock_out,Automation4_TM_stock_out])

df['updated_at']= np.nan
df.iloc[0,df.columns.get_loc('updated_at')]=dt.datetime.now()


import os
cwd = os.getcwd()
os.chdir("/home/lab/Documents/rampreet")
from googleapiclient.discovery import build
from google.oauth2 import service_account
SERVICE_ACCOUNT_FILE = 'gkey.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds= None
creds=service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE,scopes =SCOPES)
SAMPLE_SPREADSHEET_ID = '16ue6IE27DWx7zyCgFyQOLucVSOoCgYVwRAC2IYKh1ic'
service = build('sheets','v4',credentials= creds)
sheet= service.spreadsheets()

final_data= df.replace(np.nan,"")
finaldata= final_data.astype('str')
kar = finaldata.to_records(index=False)
data0 = [tuple(x) for x in kar]
data = {'values' : data0}
sheet_range = 'Gaadi!A2:H' # check the sheet range

service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=sheet_range, body={}).execute()
print("clearing the spreadsheet")

service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                       range=sheet_range, body=data, valueInputOption='USER_ENTERED').execute()
print("updating the spreadsheet")

final_data= df2.replace(np.nan,"")
finaldata= final_data.astype('str')
kar = finaldata.to_records(index=False)
data0 = [tuple(x) for x in kar]
data = {'values' : data0}
sheet_range = 'TM_and_B2c_allocated!A2:G' # check the sheet range

service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=sheet_range, body={}).execute()
print("clearing the spreadsheet")

service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                       range=sheet_range, body=data, valueInputOption='USER_ENTERED').execute()
print("updating the spreadsheet")






