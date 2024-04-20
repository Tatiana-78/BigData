#!/bin/bash
hive --hiveconf db=$1 -f query.sql
