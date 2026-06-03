# ODPS JDBC Benchmark Client

A simple benchmark client for running TPC-DS/TPC-H queries against MaxCompute via ODPS JDBC.

## Build

```bash
sh build.sh
```

## Usage

1. Copy `config.ini.template` to `config.ini` and fill in your credentials:

```ini
end_point=http://service.cn-shanghai.maxcompute.aliyun.com/api
project_name=your_project
quota_name=your_quota
access_id=your_access_id
access_key=your_access_key
```

2. Prepare your SQL query files and list them in `index_file.txt` (one file path per line).

3. Run:

```bash
java -jar target/benchmark-client-1.0.jar \
  -c config.ini \
  -s setting.properties \
  -i index_file.txt \
  > std.out 2>err.out
```

### Options

| Flag | Description |
|------|-------------|
| `-c` | Config file path (endpoint, AK/SK, project, quota) |
| `-s` | Global setting properties file (optional) |
| `-i` | Index file listing query SQL file paths |

## Output

- **stdout**: Query results (file name, SQL, result set)
- **stderr**: Execution metadata (logview URL, start/end time, cost)

## Scripts

- `scripts/get_query_running_info.py` — Extract timing summary from stderr output
- `scripts/split_jdbc_err_log.py` — Split stderr log into per-query files
- `scripts/split_jdbc_out_log.py` — Split stdout log into per-query files

Example:
```bash
python scripts/get_query_running_info.py err.out stream_0.log
```
