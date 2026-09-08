# 验证记录

执行日期：2026-09-08；macOS aarch64 / Maven 3.9.16 / Homebrew OpenJDK 26.0.2.1。

## jdbc-final.log

`mvn -B -f <jdbc>/pom.xml package -Dtest=DependencyCompatibilityTest -Dmaven.javadoc.skip=true`

```text
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.970 s -- in com.aliyun.odps.jdbc.DependencyCompatibilityTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
[INFO] Total time:  26.035 s
[INFO] Finished at: 2026-09-08T14:18:49+08:00
```

## sdk-final.log

`mvn -B -f <sdk>/pom.xml -pl odps-sdk/odps-sdk-core -am package -Dtest=DependencyCompatibilityTest,ProtobufRecordStreamVectorTest -Dsurefire.failIfNoSpecifiedTests=false -Dcheckstyle.skip -Dmaven.javadoc.skip=true`

```text
[INFO] Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.181 s - in com.aliyun.odps.tunnel.io.ProtobufRecordStreamVectorTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.716 s - in com.aliyun.odps.tunnel.io.DependencyCompatibilityTest
[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
[INFO] Total time:  24.916 s
[INFO] Finished at: 2026-09-08T14:18:48+08:00
```

## sdk-all-final.log

`mvn -B -f <sdk>/pom.xml package -DskipTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip`

```text
[INFO] BUILD SUCCESS
[INFO] Total time:  27.593 s
[INFO] Finished at: 2026-09-08T14:21:40+08:00
```

## protobuf-upgrade.log

`临时将 protobuf 改为3.25.5: mvn -B -f <sdk>/pom.xml -pl odps-sdk/odps-lot-proto -am clean compile -DskipTests -Dcheckstyle.skip`

```text
[INFO] BUILD SUCCESS
[INFO] Total time:  3.380 s
[INFO] Finished at: 2026-09-08T14:14:00+08:00
```

## protobuf-full-upgrade.log

`临时将 protobuf 改为3.25.5: mvn -B -f <sdk>/pom.xml package -DskipTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip`

```text
[INFO] BUILD SUCCESS
[INFO] Total time:  33.217 s
[INFO] Finished at: 2026-09-08T14:15:42+08:00
```

## 发行 JAR 冒烟

`/opt/homebrew/opt/openjdk/bin/javac -cp <jdbc>/target/odps-jdbc-3.10.12.jar ShadedSmoke.java`

`/opt/homebrew/opt/openjdk/bin/java -cp <jdbc>/target/odps-jdbc-3.10.12.jar:<smoke目录> ShadedSmoke`

退出 0：PASS shaded Netty buffer, shaded protobuf scalar, LZ4 frame round-trip。JDK 26 报 Unsafe/native-access 弃用警告，未影响本轮通过。

没有执行真实服务或 Java 8/21 矩阵；跳过测试的 package 仅为构建证据。protobuf 临时构建成功后已恢复2.4.1并重新构建，避免把实验制品当最终产物。
