
### SQL Query that calculates the margin of the classifieds per classified type, payment type and currency for a given period of time.
### The time period is defined in the where clause
### Additionally, the count of the classifieds and the total price and payment_cost are shown as well as the min and max creation datetimes
### George Papachrysanthou, 29/05/2019
SELECT ad_type,
	payment_type,
	currency,
	COUNT(*) rec_count,
	MIN(created_at) min_created_at,
	MAX(created_at) max_created_at,
	ROUND(SUM(price),2) price,
	ROUND(SUM(payment_cost),2) payment_cost,
	ROUND(100*SUM(price-payment_cost)/SUM(price),2) margin
FROM `pdataengineer`.`Classifieds`
WHERE created_at BETWEEN '2019-05-23 12:30:00' AND '2019-05-23 21:30:59' 
	AND ad_type<>'Free'
GROUP BY ad_type, payment_type, currency
ORDER BY 1, 2, 3;
