#!/usr/bin/env bash

JAR=`find . -maxdepth 2 -name 'odps-jdbc-*-jar-with-dependencies.jar'`
echo "JDBC Jar  : $JAR"
exec java -cp "$JAR" com.aliyun.odps.jdbc.JdbcTest "$@"
