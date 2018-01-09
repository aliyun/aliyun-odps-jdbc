#!/usr/bin/env bash

exec java -cp `find . -maxdepth 2 -name 'odps-jdbc-*-jar-with-dependencies.jar'` com.aliyun.odps.jdbc.JdbcTest "$@"
