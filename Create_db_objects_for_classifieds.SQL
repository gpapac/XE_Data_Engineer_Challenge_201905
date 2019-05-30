
### SQL Commands to create the tables, procedure and event for the processing of the Classifieds read from the Kafka server
### George Papachrysanthou, 29/05/2019

### Create the table for inserting the classifieds
### The id of each classified is defined as Primary KEY
### Two more indexes are created on the fields that are often used in where conditions in queries
CREATE TABLE `Classifieds` (
  `id` varchar(40) NOT NULL,
  `customer_id` varchar(40) NOT NULL,
  `created_at` datetime NOT NULL,
  `text` varchar(2000) DEFAULT NULL,
  `ad_type` varchar(10) NOT NULL,
  `price` float DEFAULT NULL,
  `currency` varchar(3) DEFAULT NULL,
  `payment_type` varchar(10) DEFAULT NULL,
  `payment_cost` float DEFAULT NULL,
  `offset` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_classifieds_created_at` (`created_at`),
  KEY `idx_classifieds_offset` (`offset`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



### Create the summary table to store aggregate data per hour, ad_type`, payment_type`
CREATE TABLE `Classifieds_aggr` (
  `created_at_hour` datetime NOT NULL,
  `ad_type` varchar(10) NOT NULL,
  `payment_type` varchar(10) NULL,
  `currency` varchar(3) NULL,
  `rec_count` int NOT NULL DEFAULT '0',
  `min_created_at` datetime DEFAULT NULL,
  `max_created_at` datetime DEFAULT NULL,
  `price` double DEFAULT NULL,
  `payment_cost` double DEFAULT NULL,
  `margin` float DEFAULT NULL,
  KEY `idx_classifieds_aggr_created_at_hour` (`created_at_hour`,`ad_type`)
);


### Create the procedure to update the aggregation table: Classifieds_aggr
DELIMITER $$
CREATE PROCEDURE `update_classifieds_aggr`()
BEGIN
    ### Procedure to update the table: Classifieds_aggr with aggregation data from the Classifieds table per hour, ad_type, payment_type and currency
    DECLARE max_created_at_hour datetime;
    
    ### We find the maximum create_at_hour in the aggregation table and delete the records having this value
    ### (since at the time of the previous update of the aggregate table, some of the records of this last hour might not have been already inserted in the Classifieds table) 
    SELECT MAX(created_at_hour) INTO max_created_at_hour
    FROM Classifieds_aggr;

	DELETE FROM Classifieds_aggr WHERE created_at_hour>=max_created_at_hour;
 
    ### Insert into the aggregation table the aggregate data after or equal to the above max_created_at_hour
	INSERT INTO Classifieds_aggr
    SELECT DATE_FORMAT(created_at,'%Y-%m-%d %H:00:00') created_at_hour,
		ad_type,
		payment_type,
		currency,
		COUNT(*) rec_count,
		MIN(created_at) min_created_at,
		MAX(created_at) max_created_at,
		SUM(price) price,
		SUM(payment_cost) payment_cost,
		SUM(price-payment_cost)/SUM(price) margin
	FROM `pdataengineer`.`Classifieds`
    WHERE created_at>=IFNULL(max_created_at_hour,MAKEDATE(1900,1))
	GROUP BY DATE_FORMAT(created_at,'%Y-%m-%d %H:00:00'), ad_type, payment_type, currency
	ORDER BY 1, 2, 3;
 

END$$
DELIMITER ;





### Create an hourly event to update the aggregation table: Classifieds_aggr
# SHOW EVENTS
# DROP EVENT update_classifieds_aggr_event
CREATE EVENT update_classifieds_aggr_event
    ON SCHEDULE EVERY 1 HOUR
    DO
      CALL update_classifieds_aggr();
	  
	  
