does not work as intended 

error when requesting job that ended in error 
bad request while ndn in progress 
ndn manual route creation (seems unrliable or I do something wrong)
lots of testing default values to remove


broker
1. executors
2. data location
3. unexecuted jobs
4. results of previously unexecuted job

executor
1. execute data
2. heartbeat capabilities (MDNS!!!!, UDP, REST?)
3. UDP updates with sequence number?

sensor?

database
1. store
2. retrieve
3. attributes like timestamp client id for query?

client
1. submit job
2. polling only through broker on database

generic node
1. id
2. uvicorn server
3. zeroconf (service info)

problem with update zeroconf