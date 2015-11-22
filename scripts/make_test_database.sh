#!/bin/bash -eux
hive -e '
DROP DATABASE IF EXISTS test_database CASCADE;
CREATE DATABASE test_database;
CREATE TABLE test_database.dummy_table (a INT);
'
