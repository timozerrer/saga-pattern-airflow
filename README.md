# Saga Pattern with Apache Airflow

I did some research on applying the saga pattern to microservice architectures.

## Saga Pattern

The saga pattern solves distributed transactions spanning multiple services and databases.
Chris Richardson provides some great content on the topic:
 https://microservices.io/patterns/data/saga.html
 
## Concept and architecture

Implemented is an artificial "TravelApp" resembling a business process around travel booking.
The system is setup as an microservice architecture.

![Concept and architecture](https://raw.githubusercontent.com/xTey/saga-pattern-airflow/master/img/travelappdesign.png)

The concept is around a business process that features a transaction spanning multiple services.
"TravelApp" creates travel bookings containing a hotel, flight and car rental component and implements individual services taking care of that aspect.
Now a booking may only consist out of these three components, but at times booking one of those might fail.
Required is a solution for managing such transaction made of local transactions in services and compensating them in case of failure.
Such transactions needs to be executed in an all-or-nothing fashion.
The saga pattern is a solution to that problem in a distributed system.
Apache Airflow is a workflow platform used in this project to orchestrate the saga.


## Setup

#### Prerequisites:
- docker 
- docker-compose [>1.20]


Clone this repo:
```
git clone https://github.com/xTey/saga-pattern-airflow.git
```
Run the application via docker-compose
```
docker-compose up -d
```
## Getting started
Interface service is as starting point of the saga.
It schedules the transaction with the orchestrator.
All activity will be streamed into the webui using socketio.
```
http://localhost # Interface 
```
It looks like this:

![Interface and example output](https://raw.githubusercontent.com/xTey/saga-pattern-airflow/master/img/output.PNG)

Apache Airflow's webinterface is accessible via:
```
http://localhost:8080
```

## Credits

Apache 2.0 Licence

Integrating [puckel's Airflow docker repo](https://github.com/puckel/docker-airflow). Thanks to puckel and all contributors.
