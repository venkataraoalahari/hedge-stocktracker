create database if not exists hedge;
use hedge;
create table if not exists hedge.stock_tracker(symbol string,timestampvalue string,price double,volume int) stored as textfile;

/***
* create table if not exists hedge.stock_transformation_temp
* (timestampvalue string,avgprice double,pricechange double,avgvol double,volumechange double,ratio double) partitioned by(symbol string);
***/

create table if not exists hedge.stock_transformation_temp
(symbol string, timestampvalue string,avgprice double,pricechange double,avgvol double,volumechange double,ratio double) stored as textfile;

create view if not exists hedge.deltadata as 
select st.symbol,st.timestampvalue,st.price,st.volume 
from hedge.stock_tracker st left outer join hedge.stock_transformation_temp stf 
on st.timestampvalue=stf.timestampvalue 
where stf.timestampvalue is null;