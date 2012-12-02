SELECT LINEITEM.SHIPMODE, ORDERS.ORDERPRIORITY, COUNT(ORDERS.ORDERPRIORITY)
FROM ORDERS inner join LINEITEM on
ORDERS.ORDERKEY = LINEITEM.ORDERKEY
WHERE (ORDERS.ORDERPRIORITY = '1-URGENT' OR ORDERS.ORDERPRIORITY = '2-HIGH'
OR ORDERS.ORDERPRIORITY = '3-MEDIUM' OR ORDERS.ORDERPRIORITY = '4-NOT SPECIFIED'
or ORDERS.ORDERPRIORITY = '5-LOW'
)
AND (LINEITEM.SHIPMODE = 'MAIL' OR LINEITEM.SHIPMODE ='SHIP')
AND LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE 
AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE 
AND LINEITEM.RECEIPTDATE >= {d '1994-01-01'}
AND LINEITEM.RECEIPTDATE < {d '1995-10-01'}
GROUP BY LINEITEM.SHIPMODE, ORDERS.ORDERPRIORITY
