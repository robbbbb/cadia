#!/bin/bash

while true; do
	find . | entr -r flask run -h 0.0.0.0 --debugger
	sleep 1
done
