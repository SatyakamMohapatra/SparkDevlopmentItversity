Hive

use satya123zx_test_retail_db;

create table customers (customers_id INT,
                        customer_fname STRING,
                        customer_lname STRING,
                        customer_email STRING,
                        customer_password STRING,
                        customer_street STRING,
                        customer_city STRING,
                        customer_state STRING,
                        customer_zipcode STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

create table orders
            (order_id INT,
             order_date STRING,
             order_customer_id INT,
             order_status STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


create table order_items (order_item_id INT,
                          order_item_order_id INT,
                          order_item_product_id INT,    order_item_quantity INT,
                          order_item_subtotal FLOAT,    order_item_product_price FLOAT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;



LOAD DATA LOCAL INPATH '/data/retail_db/customers' INTO TABLE customers;

INNER JOIN

SELECT C.customers_id,
       C.customer_fname,
       O.order_id,
       O.order_status 
FROM CUSTOMERS C JOIN ORDERS O
ON C.CUSTOMERS_ID = O.ORDER_CUSTOMER_ID
LIMIT 10;

LEFT OUTER JOIN

SELECT C.customers_id,
       C.customer_fname
FROM CUSTOMERS C LEFT OUTER JOIN ORDERS O
ON C.CUSTOMERS_ID = O.ORDER_CUSTOMER_ID
WHERE O.ORDER_CUSTOMER_ID IS NULL
LIMIT 10;

SELECT ORDER_STATUS,COUNT(1) FROM ORDERS
GROUP BY ORDER_STATUS;

SELECT O.ORDER_DATE,O.ORDER_STATUS,
       ROUND(SUM(OI.ORDER_ITEM_SUBTOTAL),2) AS ORDER_REVENUE
FROM ORDERS O JOIN ORDER_ITEMS OI
ON O.ORDER_ID = OI.ORDER_ITEM_ORDER_ID
WHERE ORDER_STATUS IN('COMPLETE','CLOSED')
GROUP BY O.ORDER_ID,O.ORDER_DATE,O.ORDER_STATUS
HAVING SUM(OI.ORDER_ITEM_SUBTOTAL) >= 1000
ORDER BY O.ORDER_DATE ASC,ORDER_REVENUE DESC;

SELECT O.ORDER_DATE,O.ORDER_STATUS,
       ROUND(SUM(OI.ORDER_ITEM_SUBTOTAL),2) AS ORDER_REVENUE
FROM ORDERS O JOIN ORDER_ITEMS OI
ON O.ORDER_ID = OI.ORDER_ITEM_ORDER_ID
WHERE ORDER_STATUS IN('COMPLETE','CLOSED')
GROUP BY O.ORDER_ID,O.ORDER_DATE,O.ORDER_STATUS
HAVING SUM(OI.ORDER_ITEM_SUBTOTAL) >= 1000
ORDER BY O.ORDER_DATE ASC,ORDER_REVENUE DESC;


SELECT O.ORDER_DATE,
       ROUND(SUM(OI.ORDER_ITEM_SUBTOTAL),2) AS ORDER_REVENUE
FROM ORDERS O JOIN ORDER_ITEMS OI
ON O.ORDER_ID = OI.ORDER_ITEM_ORDER_ID
WHERE ORDER_STATUS IN('COMPLETE','CLOSED')
GROUP BY O.ORDER_DATE
ORDER BY ;

CREATE TABLE REVENUE_BY_ORDER_ID (ORDER_ID INT,
                                  ORDER_DATE STRING,
                                  ORDER_STATUS STRING,
                                  ORDER_ITEM_SUBTOTAL FLOAT,
                                  ORDER_REVENUE FLOAT,
                                  DENSE_RANK_REVENUE INT,
                                  RN_ORDERBY_REVENUE INT,
                                  LEAD FLOAT,
                                  LAG FLOAT,
                                  FIRST_VALUE FLOAT,
                                  LAST_VALUE FLOAT)
                                  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                                  STORED AS TEXTFILE;
DROP TABLE REVENUE_BY_ORDER_ID;
use satya123zx_test_retail_db;
INSERT INTO TABLE REVENUE_BY_ORDER_ID
SELECT * from(
SELECT FOO.ORDER_ID,FOO.ORDER_DATE,FOO.ORDER_STATUS,FOO.ORDER_ITEM_SUBTOTAL,FOO.ORDER_REVENUE,FOO.DENSE_RANK_REVENUE,FOO.RN_ORDERBY_REVENUE,FOO.LEAD,FOO.LAG,FOO.FIRST_VALUE,FOO.LAST_VALUE FROM (
SELECT O.ORDER_ID,O.ORDER_DATE,O.ORDER_STATUS,OI.ORDER_ITEM_SUBTOTAL,
ROUND(SUM(OI.ORDER_ITEM_SUBTOTAL) OVER (PARTITION BY O.ORDER_ID),2) ORDER_REVENUE,
OI.ORDER_ITEM_SUBTOTAL/ROUND(SUM(OI.ORDER_ITEM_SUBTOTAL) OVER (PARTITION BY O.ORDER_ID),2) PCT,
rank() over (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) AS RANK_REVENUE,
DENSE_rank() over (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) AS DENSE_RANK_REVENUE,
PERCENT_rank() over (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) AS PCT_rank_REVENUE,
ROW_NUMBER() over (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) AS RN_ORDERBY_REVENUE,
LEAD(OI.ORDER_ITEM_SUBTOTAL) OVER (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) LEAD,
LAG(OI.ORDER_ITEM_SUBTOTAL) OVER (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) LAG,
FIRST_VALUE(OI.ORDER_ITEM_SUBTOTAL)  OVER (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC) FIRST_VALUE,
LAST_VALUE(OI.ORDER_ITEM_SUBTOTAL)  OVER (PARTITION BY O.ORDER_ID ORDER BY OI.ORDER_ITEM_SUBTOTAL DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)LAST_VALUE
FROM ORDERS O JOIN ORDER_ITEMS OI
ON O.ORDER_ID = OI.ORDER_ITEM_ORDER_ID
WHERE ORDER_STATUS IN('COMPLETE','CLOSED')) AS FOO
WHERE  FOO.ORDER_REVENUE >= 1000
ORDER BY FOO.ORDER_DATE ASC,FOO.ORDER_REVENUE DESC,RN_ORDERBY_REVENUE) as boo;

SELECT * FROM satya123zx_test_retail_db.REVENUE_BY_ORDER_ID LIMIT 10;

/apps/hive/warehouse/satya123zx_test_retail_db.db/revenue_by_order_id
val text = sqlContext.load().text("/apps/hive/warehouse/satya123zx_test_retail_db.db/revenue_by_order_id");
sqlContext.load("SELECT * FROM satya123zx_test_retail_db.REVENUE_BY_ORDER_ID").show()

val ordersRDD = sc.textFile("/public/retail_db/orders")
ordersRDD.take(10).foreach(println)
val orderDF = ordersRDD.map(order=>{
      (order.split(",")(0).toInt,order.split(",")(1),
      order.split(",")(2).toInt,order.split(",")(3))}).toDF("order_id","order_date","order_customer_id","order_status");

import scala.io.Source
val productRaw = Source.fromFile("/data/retail_db/products/part-00000").getLines().toList
val productRDD = sc.parallelize(productRaw);
val productDF =productRDD.map(rec=> (rec.split(",")(0).toInt,rec.split(",")(2))).toDF("product_id","product_name");
productDF.take(10).foreach(println)
productDF.registerTempTable("products")


sqlContext.sql("use satya123zx_test_retail_db");
sqlContext.setConf("spark.sql.shuffle.partitions", "4")
sqlContext.sql("select o.order_date,p.product_name,sum(oi.order_item_subtotal) daily_revenue_per_product "+
"from orders o join order_items oi on o.order_id = oi.order_item_order_id "+
"join products p on oi.order_item_product_id = p.product_id "+
"where o.order_status in ('COMPLETE','CLOSED') "+
"group by o.order_date,p.product_name "+
"order by o.order_date,daily_revenue_per_product DESC").show()

sqlContext.sql("CREATE DATABASE satya123zx_sparkDF_retailDB");
sqlContext.sql("CREATE TABLE satya123zx_sparkDF_retailDB.daily_revenue_per_product (order_date string,product_name string,daily_revenue_per_product float)");
sqlContext.sql("SELECT * from  satya123zx_sparkDF_retailDB.daily_revenue_per_product");
sqlContext.setConf("spark.sql.shuffle.partitions", "4")
sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +order_date
product_name"daily_revenue_per_productOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') " +
"GROUP BY o.order_date, p.product_name " +
"ORDER BY o.order_date, daily_revenue_per_product desc").
show
productDF.insertInto("satya123zx_sparkDF_retailDB.daily_revenue_per_product")
sqlContext.sql("SELECT * from  satya123zx_sparkDF_retailDB.daily_revenue_per_product");
sqlContext.sql("SELECT * from  daily_revenue_per_product_test").show;
use satya123zx_sparkDF_retailDB;



