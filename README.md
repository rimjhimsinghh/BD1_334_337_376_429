# BD1_334_337_376_429  

## Yet Another Kafka

There are 4 main python files.  
- producer.py  
- consumer.py  
- broker.py
- zookeeper.py

### Commands to run the above files

To run `zookeeper.py`
```
python3 zookeeper.py
```

To run the 3 `broker.py`
```
python3 broker.py 4456 9091
python3 broker.py 4457 9092
python3 broker.py 4458 9093
```
Close Terminal for Closing Broker

To run the 3 `producer.py`
```
python3 producer.py <TOPIC_NAME>
```
Ctrl+C to close producer

To run the 3 `consumer.py`
```
python3 consumer.py <TOPIC_NAME> --from-beginning
```
OR
```
python3 consumer.py <TOPIC_NAME>
```
Ctrl+C to close producer
