#!/bin/bash
nohup python3 mqtt_receiver.py > std.out 2>std.err &
