#!/bin/sh

# usage: externalpull.sh <lfn> <pfn>

# This is an example file puller script, that creates a fake file <pfn>
# by pulling it from nowhere using dd
#
# If querying an external system, the query should be based on the <lfn>
#
# This script also has the reponsibility of purging some files if
# there is not enough space in the mountpoint hosting <pfn>
# The freespace can be calculated using 'df'
# Replica selection can be random, and the replica has to be deleted using dome_delreplica
# towards the dome head node
#

sleep 5
dd "of=$2" "if=/dev/urandom" bs=123456 count=1


