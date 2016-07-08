#!/bin/sh

# usage: externalpull.sh <lfn> <pfn>

# This is an example file puller script, that creates a fake file <pfn>
# by pulling it from nowhere using dd
#
# If querying an external system, the query should be based on the <lfn>

sleep 5
dd "of=$2" "if=/dev/urandom" bs=123456 count=1


