## XE Data Engineer Code Challenge implementation by George Papachrysanthou
Date: 29/05/2019

For this challenge I developed a python program that connects to the Kafka server given and reads the messages published there under the specific topic.
Each message is a JSON document that is parsed and then stored in a MySQL database table, in the MySQL server given.
The files submitted are the following:

<ol>
<li><b>Create_db_objects_for_classifieds.SQL</b>: The creation scripts for all the necessary objects in the MySQL database (although all these are already created in the given database)</li>
<li><b>XE_DataEngineer_Challenge.py</b>: The Python program that connects to the Kafka server, reads the messages for the topic given and stores the relevant records in the MySQL database table  </li>
<li><b>Calculate_classifieds_margin.SQL</b>: SQL query to calculate the margin (and other aggregate values) of all the classifieds stored in the database, for a given period of time, grouped by classified type, payment type and currency </li>
</ol>


Some additional comments for the implementation:

* The connection settings for the Kafka and MySQL database servers are defined in the python program

* All the files contain helpfull comments

* The Python program logs the warnings and errors in a log file (XE_DataEng_Chal.log)


