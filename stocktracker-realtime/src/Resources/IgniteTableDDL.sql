create table stock 
(symbol varchar,timestampvalue varchar,price double,volume double,priceavg double,pricechange double,
 volumeavg double,volumechange double,ratio double, insertFlag timestamp,primary key (symbol, timestampvalue)  );

create table stock_transformation 
(symbol varchar,timestampvalue varchar,avgprice double,pricechange double,avgvolume double,
volumechange double,ratio double,histratio double, primary key (symbol, timestampvalue));

create table stock_alert(symbol varchar,timestampvalue varchar,histratio double,currentratio double,
 ratiodeviation double,threshold double,primary key (symbol, timestampvalue));

 
/*** 
*create table stock 
*(symbol varchar,timestampvalue varchar,price double,volume double,priceavg double,pricechange double,
*volumeavg double,volumechange double,ratio double, insertFlag timestamp,primary key (symbol, timestampvalue)  )with "template=replicated";

*create table stock_transformation 
*(symbol varchar,timestampvalue varchar,avgprice double,pricechange double,avgvolume double,
*volumechange double,ratio double,histratio double, primary key (symbol, timestampvalue)) with "template=replicated";

*create table stock_alert(symbol varchar,timestampvalue varchar,histratio double,currentratio double,
*ratiodeviation double,threshold double,primary key (symbol, timestampvalue)) with "template=replicated";
***/ 