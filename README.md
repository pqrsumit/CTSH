# CTSH

This repository was created to share the code during demos. 
Kafaka is used as message broker and can  be downloaded directly from https://kafka.apache.org/quickstart
This set up will help to run standlone Kafka cluster in windows machine to simulate streaming pipeline. 

CTS_Demo_Data_Generator.py 
This is a python program to generate random data related to Flights/Passengers/Trips. It will create 3 files 

Aircrafts.txt - Infomration about randomly generated aircraft data 
Passengers.json - Information about passengers 
Tips.csv :- Trip booking transctions 

These files maintain referential integrity using key columns like TailNumber/Passport Number. 
