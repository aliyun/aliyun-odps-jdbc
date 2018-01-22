#!/usr/bin/env bash

JAR=`find . -maxdepth 2 -name 'odps-jdbc-*-jar-with-dependencies.jar'`
echo "JDBC Jar  : $JAR"
exec java -cp "$JAR:logback/logback-core-1.2.3.jar:logback/logback-classic-1.2.3.jar" com.aliyun.odps.jdbc.JdbcTest "$@"
