#!/bin/bash    
fifo_name="/tmp/peek.fifo"
while true
do
    if read line; then
        echo $line
    fi
done <"$fifo_name"
