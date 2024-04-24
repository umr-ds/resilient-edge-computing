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
2. heartbeat capabilities (MDNS!!!!, UDP, REST?) push to broker
3. UDP updates with sequence number?

sensor?

database
1. store
2. retrieve
3. attributes like timestamp client id for query?
4. special space for results

client
1. submit job
2. polling only through broker on database

generic node
1. id
2. uvicorn server
3. zeroconf (service info)

problem with update zeroconf

data names as arbitrary paths

pagination query for specific data path structured like job_id name ...

# Questions
1. who decides on result names? broker or executor
2. results published as zip? under special prefix? name = job_id
3. results published separate under different names? yes
4. how to propagate changes to node state (MDNS does not work well)?
5. how to handle errors? later
6. orphans?
7. restore state on restart? crash
8. get queued job result poll broker on job(cheaper) or on result name(simpler interface)? no
9. keep flexible (complicated and efficient) ways or simplify to single system? simplify
10. use interfaces (like every executor can act like datastore for data needed for execution)?
11. queue job on executor if resources are exhausted on start? egal
12. how and when is data deleted? delete oldest keep with flag