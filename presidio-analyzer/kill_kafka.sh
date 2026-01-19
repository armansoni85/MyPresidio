#!/bin/bash
echo "Killing all Kafka processes..."
for pid in $(ps aux | grep kafka | grep -v grep | awk '{print $2}'); do
	  kill -9 $pid
	    echo "Killed process $pid"
    done
    echo "All Kafka processes terminated."
