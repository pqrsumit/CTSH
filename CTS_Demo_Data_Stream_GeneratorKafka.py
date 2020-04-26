# Author:- Sumit Bunage
# Purpose:- A Simple Python Program to Geenrate Dummy Flight Data for demo Purpose

# import necessary package
import time
import base64
import gzip
import random
import uuid
import datetime
import os
import json
from kafka import KafkaProducer


Basepath = "F:/Spark-Data/data-master/CTS-Demo/Scripts-Commands/CTSH/"

# Constant/List etc to generate dummy data
PassengerFirstName = ["Jake","Billy","Emily","Eric","Robert","Mary","Heather","Phil","Russell","Nicole"]
PassengerLastName = ["Greene","Anderson","Brown","Hawkins ","Richmond","Constantine","Coulson","Fury","Stark","Banner"]
PassengerGender = ["M","F"]
Alpha = ["A","B","C","D","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
Origin = ["ATL","LAX","ORD","DFW","DEN","JFK","SFO","SEA","LAS","MCO"]
Destination = ["EWR","CLT","PHX","IAH","MIA","BOS","MSP","FLL","DTW","PHL"]
AircraftName = ["Boeing-747","Boeing-767","Boeing-777","Boeing-787","Airbus-A300","Airbus-A330","Airbus-A340","Airbus-A350","Airbus-A380"]


# Individual module to generate data
Passengers = []
def GenPassengers():
    # print("Nothing")
    for i in range(0,4):
        # print(i)
        f = random.choice(PassengerFirstName)
        l = random.choice(PassengerLastName)
        g = random.choice(PassengerGender)
        # bd = str(datetime.datetime.now()- datetime.timedelta(weeks=random.randint(1000,2000)))[0:10] #normal String
        bd = str(base64.b64encode(bytes(str(datetime.datetime.now() - datetime.timedelta(weeks=random.randint(1000, 2000)))[0:10]
                                    ,"utf-8")))[1:].replace("'","") #encoded
        # print(bd)
        passport = random.choice(Alpha)+"-"+ str(random.randint(6683348,77456612))
        mealPref = random.choice(["Veg","Non-Veg","Any"])
        someint = random.randint(1,100)
        p = {"FirstName":f,"LastName":l,"randomint":someint,"MoreDetails":{"Passport":passport,"PersonalDetails":{"Gender":g,"DOB":bd,"MealPreference":mealPref}}}
        # print(p)
        Passengers.append(p)
    with open(Basepath+"Passengers.json","w") as fh:
        for i in Passengers:
            data = json.dumps(i)+"\n"
            fh.writelines(data)
    fh.close()

Aircrafts = []
def AirCraftDetail():
    for i in range(0,20):
        v = random.choice(AircraftName) #vehicle
        c = random.randint(20,250) #Passenger Capacity
        t = random.choice(Alpha)+ str(random.randint(2121,3253)) #Tail Number
        # ad = v+" "+str(c)+" "+t #vehiclename Passenger Capacity Tail Number
        add = {"AircraftName":v,"Capacity":c,"TailNumber":t}
        Aircrafts.append(add)

    with open(Basepath+"Aircrafts.txt","w") as fh:
        fh.write("AircraftName\tCapacity\tTailNumber\n")
        for i in Aircrafts:

            v = i["AircraftName"]
            c = i["Capacity"]
            t = i["TailNumber"]
            # print(str(i.values()))
            data = v+"\t"+str(c)+"\t"+t+"\n"
            fh.writelines(data)
    fh.close()

# Function to generate Flight Journey details
def GenTripDetails():
    # print("Nothing")
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))
    counter = random.randint(5,20)
    while(counter>0):
        P = random.choice(Passengers)
        A = random.choice(Aircrafts)
        O = random.choice(Origin)  # Origin
        D = random.choice(Destination)  # Dest
        TD = str(datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 365)))[0:10]  # TravelDate
        T = str(random.randint(1, 10))  # Hours Travelled
        R = str(round(random.uniform(50.00, 500.00), 2))  # Revenue
        sep = ","
        data = A["TailNumber"] + sep \
               + O + sep + D + sep + TD + sep + T + sep + R + sep \
               + A["AircraftName"] + sep \
               + P["FirstName"] + sep \
               + P["LastName"] + sep \
               + P["MoreDetails"]["Passport"]
        print(data)
        producer.send('test',value=data,key=bytes(O+" "+D,'utf-8')#,headers=[('content-encoding', b'base64')]
                              )
        counter = counter -1
    time.sleep(random.randint(5,8))
    # with open(Basepath+"Trips.csv","w") as fh:
    #     fh.writelines("TailNumber,Origin,Destination,TravelDate,TravelTime,SeatPrice,Capacity,AircraftName,FirstName,LastName,DOB\n")
    #
    #         fh.writelines(data)
    #     fh.close()




# To get passenger details
GenPassengers()
# print(Passengers)
AirCraftDetail()
# print(Aircrafts)

while(True):
    GenTripDetails()
