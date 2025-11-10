import re
import pandas as pd
import os

# --- 配置区 ---
# 日志文件路径
LOG_FILE_PATH = '' # 将您的日志文件放在这里
# 输出报告的文件名
OUTPUT_CSV_FILE = 'jdbc_analysis_report.csv'

# --- 正则表达式定义 ---
re_run_sql = re.compile(r"\[(connection-\d+)\] Run SQL: (.*?);")
re_instance_id = re.compile(r"\[(connection-\d+)\] InstanceId: ([\w_]+)")
re_run_time = re.compile(r"It took me (\d+) ms to run sql, instanceId: ([\w_]+)")
re_fetch_time = re.compile(r"\[(connection-\d+)\] It took me (\d+) ms to fetch")

def analyze_jdbc_log(log_data):
    """
    分析JDBC日志，提取查询耗时
    """
    lines = log_data.strip().split('\n')
    results = {}
    last_info_on_connection = {}

    for line in lines:
        match_run_sql = re_run_sql.search(line)
        if match_run_sql:
            conn_id, sql_text = match_run_sql.groups()
            if conn_id not in last_info_on_connection:
                last_info_on_connection[conn_id] = {}
            last_info_on_connection[conn_id]['last_sql'] = sql_text.strip()

        match_instance_id = re_instance_id.search(line)
        if match_instance_id:
            conn_id, instance_id = match_instance_id.groups()
            if conn_id not in last_info_on_connection:
                last_info_on_connection[conn_id] = {}
            last_info_on_connection[conn_id]['last_instance'] = instance_id

            if instance_id not in results:
                sql = last_info_on_connection.get(conn_id, {}).get('last_sql', 'Unknown')
                results[instance_id] = {"sql": sql, "run_ms": None, "fetch_ms": None}

        match_run_time = re_run_time.search(line)
        if match_run_time:
            time_ms, instance_id = match_run_time.groups()
            if instance_id not in results:
                results[instance_id] = {"sql": "Unknown", "run_ms": None, "fetch_ms": None}
            results[instance_id]["run_ms"] = int(time_ms)

        match_fetch_time = re_fetch_time.search(line)
        if match_fetch_time:
            conn_id, time_ms = match_fetch_time.groups()
            if conn_id in last_info_on_connection and 'last_instance' in last_info_on_connection[conn_id]:
                last_instance_id = last_info_on_connection[conn_id]['last_instance']
                if last_instance_id in results:
                    results[last_instance_id]["fetch_ms"] = int(time_ms)

    if not results:
        print("在日志中未找到任何有效的耗时记录。")
        return None

    df = pd.DataFrame.from_dict(results, orient='index')
    df.index.name = 'InstanceId'
    df = df.reset_index()

    # 将run_ms和fetch_ms列转换为数值类型，无法转换的设为NaN
    df['run_ms'] = pd.to_numeric(df['run_ms'], errors='coerce')
    df['fetch_ms'] = pd.to_numeric(df['fetch_ms'], errors='coerce')

    return df

def process_and_save_results(df):
    """
    以表格形式展示分析结果、保存到文件并打印统计信息
    """
    if df is None or df.empty:
        return

    # --- 1. 输出耗时表格到控制台 ---
    print("--- 查询耗时分析表格 ---")
    # 为了显示更美观，将NaN替换为'-'进行打印
    print(df.fillna('-').to_string(index=False))

    # --- 2. 将结果保存到CSV文件 ---
    try:
        # 使用 utf_8_sig 编码确保Excel能正确打开包含中文的CSV文件
        df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf_8_sig')
        print(f"\n✅ 分析结果已成功保存到文件: {OUTPUT_CSV_FILE}")
    except Exception as e:
        print(f"\n❌ 保存文件失败: {e}")

    # --- 3. 计算并打印统计数据 ---
    print("\n--- 耗时统计分析 (单位: 毫秒) ---")

    # 选择需要分析的数值列
    timing_df = df[['run_ms', 'fetch_ms']]

    # 使用 describe 方法计算常用统计量，并指定需要的百分位数
    stats = timing_df.describe(percentiles=[.50, .95, .99]).rename(index={'50%': 'p50(median)'})

    # 调整输出格式，使其更易读
    pd.options.display.float_format = '{:.2f}'.format

    print(stats)
    print("\n" + "="*50 + "\n")
    print("说明:")
    print("  count: 计数")
    print("  mean: 平均值")
    print("  std: 标准差")
    print("  min: 最小值")
    print("  p50(median): 中位数 (50%的数据小于此值)")
    print("  p95: 95百分位数 (95%的数据小于此值)")
    print("  p99: 99百分位数 (99%的数据小于此值)")
    print("  max: 最大值")


if __name__ == "__main__":
    log_content_to_analyze = ""
    try:
        # 使用 errors='ignore' 来跳过无法解码的字符，增强兼容性
        with open(LOG_FILE_PATH, 'r', encoding='utf-8', errors='ignore') as f:
            print(f"正在分析日志文件: {LOG_FILE_PATH} (使用 utf-8 编码, 已忽略解码错误)")
            log_content_to_analyze = f.read()

    except FileNotFoundError:
        print(f"警告: 未找到日志文件 '{LOG_FILE_PATH}'。将使用脚本内嵌的示例日志内容进行演示。")
        log_content_to_analyze = LOG_CONTENT
    except Exception as e:
        print(f"读取文件时发生意外错误: {e}")

    if log_content_to_analyze:
        analysis_df = analyze_jdbc_log(log_content_to_analyze)
        process_and_save_results(analysis_df)
    else:
        print("没有可供分析的日志内容。")

