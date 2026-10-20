# JDBC 3.10.16

Expose `enableTypedResult` (JDBC URL) and `enable_typed_result` (Properties / odps_config). The default remains `true`; the configured value is retained when rebuilding executors.

Set `enableTypedResult=false` to return String values and STRING metadata from MaxQA direct CSV results. This does not force all JDBC results to strings: Tunnel results, including automatic Tunnel fallback for truncated direct results, retain their actual types. Untyped direct CSV returns the null marker as text. Only `true` and `false` are accepted (case insensitive).

```text
jdbc:odps:https://service.<region>.maxcompute.aliyun.com/api?project=<project>&enableTypedResult=false
```

通过 URL 的 `enableTypedResult` 或 Properties / 配置文件的 `enable_typed_result` 配置，默认仍为 true。关闭仅影响 MaxQA direct CSV；Tunnel 结果（包括截断后自动转 Tunnel）仍按实际类型返回，CSV 空值标记以文本返回。

This patch retains SDK 0.58.1-public and the existing shaded packaging from 3.10.15.
