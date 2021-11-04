# Kafka
This User Target sends the Adabas data as JSON messages to Kafka.

## Build
Change directory to the example root directory and enter on a command prompt
```
gradle installDist
```
The jar with all dependencies will be in build/install directory.

## Install
The *kafka-clients-2.3.0.jar* from install/kafka folder must be copied to the <art root>/art/data/webapps/sqlrep/WEB-INF/lib folder that the Kafka User Target is working properly.

## Disclaimer
Utilities and samples shown here are not official parts of the Software AG products. These utilities and samples are not eligible for technical assistance through Software AG Global Support. Software AG makes no guarantees pertaining to the functionality, scalability , robustness, or degree of testing of these utilities and samples. Customers are strongly advised to consider these utilities and samples as "working examples" from which they should build and test their own solutions. 

