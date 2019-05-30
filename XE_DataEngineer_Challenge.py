
### Python application that connects to the Kafka topic 'data' and consumes the JSON documents that arrive there.
### Each document is inserted in a MySQL database in a table called Classifieds.
### Every time it starts it reads the maximum offset of the records already in the Classifieds table, and starts reading the Kafka messages after this offset (so it doesn't use Kafka for offset persistence).
### The process runs continuously. A timeout is used for the Kafka consumer in order to stop temporarily the reading of new messages if no new messages arrive for some time.
### In this case it waits for [var_retry_after] seconds and then retry reading messages from Kafka.
### The above variables are defined at the beginning of main method and can be adjusted according to the final environment.
### The database connection settings are defined in the corresponding line of the function: get_db_connection
### The Kafka connection settings are defined in the corresponding line of the main method.
### The program logs the errors and warnings in a log file XE_DataEng_Chal.log in the same directory as this program.
### If a Kafka message is not a valid JSON document, the relevant error is logged together with the Kafka message, and the program continues to the next message.
### Also if a MySQL Integrity Error is encountered during the insert of a record, due to a record with the same id (since id is unique in the database table), the error is logged and the program continues to the next message.

### Developer: George Papachrysanthou
### Development date: 29/05/2019


import logging
import time
import json
import sys
from json import loads
from kafka import KafkaConsumer
from kafka import TopicPartition

import mysql.connector
from datetime import datetime
 
_connection = None

def get_db_connection():
    ### function that connects to mysql db and returns the connection
    global _connection
    if not _connection:
        try:
            _connection = mysql.connector.connect(host='pdataengineer.cg2t1fioak49.eu-west-3.rds.amazonaws.com',database='pdataengineer',user='root',password='aHYczeREQtqRncGmds6KSQ4VVDm2pNHg')
            if _connection.is_connected():
                db_Info = _connection.get_server_info()
                print("Connected to MySQL database server ",db_Info)
        except mysql.connector.Error  as err:
            logging.error("MySQL Connector Error: "+str(err.errno) + " " + str(err.msg))
            print("MySQL Connector Error:" + str(err.errno) + " " + str(err.msg))
            sys.exit(1)
    return _connection 

def close_db_connection():
    ### function that closes the global db connection if it exists and is connected
    global _connection
    if not(_connection is None):
        if _connection.is_connected():
            print("Closing db connection")
            _connection.close()
        _connection=None


def read_max_db_offset():
    ### function that finds and returns the maximum offset value already stored in the database table: "Classifieds"
    ### If the table has no records, the function returns: -1
    max_db_offset=-1
    db = get_db_connection()
    cursor = db.cursor()
    try:
        cursor.execute("SELECT MAX(offset) FROM Classifieds")
        # print(cursor.rowcount)
        row = cursor.fetchone()
        # print(row)
        if any(field is None for field in row):
            max_db_offset=-1
        else:
            max_db_offset=row[0]
    except mysql.connector.Error  as err:
            logging.error("MySQL Connector Error while reading MAX(offset) FROM Classifieds: "+str(err.errno) + " " + str(err.msg))
            print("MySQL Connector Error while reading MAX(offset) FROM Classifieds:" + str(err.errno) + " " + str(err.msg))
            sys.exit(1)
    cursor.close()
    close_db_connection
    return max_db_offset
  
def main():
    
    ### The values of many of the variables below, will have to be adjusted according to the final environment.
    ### Define the timeout for waiting for new Kafka messages
    var_kafka_consumer_timeout=5000 # 5000=5seconds
    ### Define the time to wait before retrying to read new messages from Kafka
    var_retry_after=20 # 20=20 seconds
    ### Define the Kafka topic
    kafka_topic='data'
    ### Define the database table in which to store the records
    table_name='Classifieds'
    ### Define every how many records to commit the changes in the database (for performance reasons we do not commit every record). Even values of 1000 are suitable for fast dbs.
    commit_every_x_recs=100
 
 
 
    last_offset_in_db=-1
    while True:
        ### Outer loop to retry fetching new records from Kafka if the consumer timeout was exceeded
        if last_offset_in_db>=0:
            print("Retrying to read Kafka consumer records")
        ### Create Kafka Consumer. We set timeout to 5 seconds in order to temporary stop the inner loop if there are no new records
        consumer = KafkaConsumer(bootstrap_servers='35.181.21.223', consumer_timeout_ms=var_kafka_consumer_timeout) 
        topic_partition_0 = TopicPartition(kafka_topic, 0)
        consumer.assign([topic_partition_0])
        if last_offset_in_db<0:
            ### if the variable last_offset_in_db is < 0, read the maximum offset from the database table
            last_offset_in_db=read_max_db_offset()
        if last_offset_in_db<0:
            ### if the variable last_offset_in_db is still < 0, start from the beggining of the records
            consumer.seek_to_beginning()
        else:
            ### if the variable last_offset_in_db is >= 0, seek the next offset in KafkaConsumer and start from there
            print("Start reading from offset: "+ str(last_offset_in_db+1))
            consumer.seek(topic_partition_0,last_offset_in_db+1)   

        ### Declare the 2 insert commands, one for the records having ad_type="Free" (from which all the fields related to the payment are missing) and one for the other classifieds
        sql_ins_free = "insert into "+table_name+"(id, customer_id, created_at, text, ad_type, offset) value(%s, %s, %s, %s, %s, %s)"
        sql_ins_not_free = "insert into "+table_name+"(id, customer_id, created_at, text, ad_type, price, currency, payment_type, payment_cost, offset) value(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        imsg=0
        imsg_inserted=0
        imsg_errors=0
        current_offset=-1
        print("Checking for new Kafka records")
        ### For all the records in KafkaConsumer
        for message in consumer:
            imsg=imsg+1
            current_offset=message.offset
            # print (message.value)
            try:
                ### parse the JSON message.value
                msg_json_data=json.loads(message.value)
            except Exception as e:
                ### if decoding the JSON message.value raises an exception, log it and continue (in the log we also export the actual message)
                imsg_errors=imsg_errors+1
                logging.error(str(imsg) + " (Message Offset: "+ str(message.offset)+", Error: "+ str(e)+")")
                logging.error(' >>>> Message related to error: ' + str(message))
                continue

            db = get_db_connection()
            is_free=-1
            try:
                ### Construct the tuple with the data to be inserted, depending on the ad_type of the classified
                if msg_json_data['ad_type']=='Free':
                    data1 =  (msg_json_data['id'], msg_json_data['customer_id'], msg_json_data['created_at'][:19], msg_json_data['text'], msg_json_data['ad_type'], message.offset)
                    is_free=1
                else:
                    data1 =  (msg_json_data['id'], msg_json_data['customer_id'], msg_json_data['created_at'][:19], msg_json_data['text'], msg_json_data['ad_type'], msg_json_data['price'], msg_json_data['currency'], msg_json_data['payment_type'], msg_json_data['payment_cost'], message.offset )
                    is_free=0
            except Exception as e:
                ### if trying to fetch one of the expected JSON fields raises an exception, log it and continue (in the log we also export the actual message)
                imsg_errors=imsg_errors+1
                logging.error(str(imsg) + " (Message Offset: "+ str(message.offset)+", Error: "+ str(e)+")")
                logging.error(' >>>> Message related to error: ' + str(message))
                continue

            try:
                cursor = db.cursor()
                ### Execute the relevant insert statement, with the tuple created above.
                if is_free==1:
                    cursor.execute(sql_ins_free, data1)
                if is_free==0:
                    cursor.execute(sql_ins_not_free, data1)
                imsg_inserted=imsg_inserted+1
                ### Commit changes to the database every X records
                ### We do not commit every record for performance reasons
                if imsg %commit_every_x_recs == 0:
                    # print("Commit changes to db")
                    db.commit()   # commit the changes
                    last_offset_in_db=current_offset
                # cursor.close()
            except mysql.connector.errors.IntegrityError as err:
                ### if a MySQL integrity error is raised during insert, log it and continue
                imsg_errors=imsg_errors+1
                logging.error("MySQL Integrity Error (inserting into table: "+table_name+") "+ str(err.errno) + " " + str(err.msg))
                continue
            except mysql.connector.Error  as err:
                ### if another MySQL or other error is encoutered (for example lost of connection), log it and break
                imsg_errors=imsg_errors+1
                logging.error("MySQL Connector Error (inserting into table: "+table_name+") "+str(err.errno) + " " + str(err.msg))
                break
            except Exception  as e:
                imsg_errors=imsg_errors+1
                logging.error(str(imsg) + " (Message Offset: "+ str(message.offset)+", Error: "+ str(e)+")")
                break

            ### The following commands can be used to simulate the case where no new messages are comming, and test the outer loop
            # if imsg >1200:
            #    break

        if imsg==0:
            print("No new Kafka messages found")
        else:
            if 'db' in locals():
                if db.is_connected():   
                    ### Since we do not commit after every insert, we issue another commit here, before closing the connection
                    # print("Commit changes to db")
                    db.commit()   # commit the changes
                    last_offset_in_db=current_offset
            print(str(imsg)+ " new Kafka messages found. " +str(imsg_inserted)+" messages inserted in the database. " +str(imsg_errors)+" messages had problems.")

        close_db_connection()

        ### Time to sleep until the next attempt to read new Kafka messages
        time.sleep(var_retry_after)

   
if __name__ == "__main__":
    ### setup logging (first remove existing handlers)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
 
    logging.basicConfig(filename='XE_DataEng_Chal.log',filemode='a',
        format='%(asctime)s: %(name)s: %(thread)d: ' +
        '%(levelname)s:%(process)d: %(message)s',
        level=logging.WARNING
    )
    main()

 
