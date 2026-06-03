
# -*- coding: utf-8 -*-
import re
import os
import sys
from datetime import datetime

def format_time_delta(start_str, end_str):
    """
    计算两个时间之间的差值，并格式化为指定格式
    """
    try:
        start_time = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")

        delta = end_time - start_time
        total_seconds = delta.total_seconds()

        # 格式化为 0:00:04 格式
        hours, remainder = divmod(int(total_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = "{}:{:02d}:{:02d}".format(hours, minutes, seconds)

        # 格式化为 4.265 s 格式（保留3位小数）
        seconds_str = "{:.3f} s".format(total_seconds)

        return time_str, seconds_str, int(total_seconds * 1000)  # 返回毫秒数
    except Exception as e:
        return "0:00:00", "0.000 s", 0

def extract_query_timing_info(input_file_path, output_file_path="stream_0.log"):
    """
    从err.out文件中提取每个查询的耗时信息，并按指定格式写入到指定文件

    Args:
        input_file_path (str): 输入的err.out文件路径
        output_file_path (str): 输出文件路径
    """

    # 检查输入文件是否存在
    if not os.path.exists(input_file_path):
        raise IOError("输入文件不存在: {}".format(input_file_path))

    # 读取整个文件内容（兼容Python 2和3）
    if sys.version_info[0] == 3:
        # Python 3
        with open(input_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        # Python 2
        import codecs
        with codecs.open(input_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

    # 使用正则表达式提取所有查询块
    file_blocks = re.split(r'fileName:', content)[1:]  # 跳过第一部分（文件头）

    # 如果没有查询块，直接返回
    if not file_blocks:
        print("未找到任何查询块")
        return

    # 解析所有查询块并提取时间信息
    query_timing_data = []

    for block in file_blocks:
        # 提取文件名
        filename_match = re.match(r'(.*?)\n', block)
        if not filename_match:
            continue
        filename = filename_match.group(1).strip()

        # 从文件路径中提取查询编号
        query_num_match = re.search(r'query(\d+)\.sql', filename)
        query_num = query_num_match.group(1) if query_num_match else "unknown"

        # 提取SQL内容以获取模板名称
        sql_match = re.search(r'SQL:\n(.*?)(?:\n\nfileName:|\Z)', block, re.DOTALL)
        sql_content = sql_match.group(1) if sql_match else ""

        # 提取查询模板名称
        #template_match = re.search(r'using template (query\d+\.tpl)', sql_content)
        #template_name = template_match.group(1) if template_match else "query{}.tpl".format(query_num)
        start_template_match = re.search(r'-- start query \d+ in stream \d+ using template (query\d+\.tpl)', sql_content)
        template_name = start_template_match.group(1) if start_template_match else "query{}.tpl".format(query_num)
        # 从模板名称中提取实际的查询编号
        template_query_num_match = re.search(r'query(\d+)\.tpl', template_name)
        actual_query_num = template_query_num_match.group(1) if template_query_num_match else query_num

        # 提取Index
        index_match = re.search(r'Index\s*:\s*(\d+)', block)
        index = index_match.group(1) if index_match else "1"

        # 提取时间信息
        starttime_match = re.search(r'starttime:([^\s]+ [^\s]+)', block)
        starttime = starttime_match.group(1) if starttime_match else "N/A"

        endtime_match = re.search(r'endtime:([^\s]+ [^\s]+)', block)
        endtime = endtime_match.group(1) if endtime_match else "N/A"

        cost_time_match = re.search(r'Cost time\s*:\s*(\d+)\s*ms', block)
        cost_time_ms = cost_time_match.group(1) if cost_time_match else "N/A"

        # 计算时间差
        if starttime != "N/A" and endtime != "N/A":
            time_format, seconds_format, calculated_ms = format_time_delta(starttime, endtime)
        else:
            time_format, seconds_format, calculated_ms = "0:00:00", "0.000 s", 0

        # 保存查询时间数据
        query_timing_data.append({
            'query_num': actual_query_num,
            'template_name': template_name,
            'index': index,
            'starttime': starttime,
            'endtime': endtime,
            'time_format': time_format,
            'seconds_format': seconds_format,
            'cost_time_ms': cost_time_ms,
            'calculated_ms': calculated_ms
        })

    # 写入输出文件（兼容Python 2和3）
    if sys.version_info[0] == 3:
        # Python 3
        with open(output_file_path, 'w', encoding='utf-8') as f:
            # 按照指定格式写入每个查询的信息
            for data in query_timing_data:
                line = "query{} start at {}, end at {}, cost {}, {}\n".format(
                    data['query_num'],
                    data['starttime'],
                    data['endtime'],
                    data['time_format'],
                    data['seconds_format']
                )
                f.write(line)
    else:
        # Python 2
        import codecs
        with codecs.open(output_file_path, 'w', encoding='utf-8') as f:
            # 按照指定格式写入每个查询的信息
            for data in query_timing_data:
                line = "query{} start at {}, end at {}, cost {}, {}\n".format(
                    data['query_num'],
                    data['starttime'],
                    data['endtime'],
                    data['time_format'],
                    data['seconds_format']
                )
                f.write(line)

    print("已成功提取 {} 个查询的耗时信息并写入 {}".format(len(query_timing_data), output_file_path))

    # 同时在控制台打印结果
    print("\n提取结果:")
    for data in query_timing_data:
        print("query{} start at {}, end at {}, cost {}, {}".format(
            data['query_num'],
            data['starttime'],
            data['endtime'],
            data['time_format'],
            data['seconds_format']
        ))

def main():
    """
    主函数，支持命令行参数
    """
    if len(sys.argv) < 2:
        print("用法: python get_query_running_info.py <input_err_file> [output_file]")
        print("示例: python get_query_running_info.py err.out stream_0.log")
        return 1

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "stream_0.log"

    try:
        extract_query_timing_info(input_file, output_file)
    except Exception as e:
        print("错误: {}".format(e))
        return 1

    return 0

if __name__ == "__main__":
    exit(main())