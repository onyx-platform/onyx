#!/bin/bash


rm run.log run_results.log

for i in `seq 100`; do
	TIMBRE_LOG_LEVEL="error" TEST_TRANSPORT_IMPL=core.async lein midje >> run.log 
	echo $? >> run_results.log
done
