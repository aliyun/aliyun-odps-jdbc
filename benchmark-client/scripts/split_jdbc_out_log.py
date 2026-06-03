# -*- coding: utf-8 -*-
import re
import os
import argparse
import sys
from collections import defaultdict

def extract_template_name(sql_content):
    """
    从SQL内容中提取模板名称
    如果找到 'using template query*.tpl' 模式，则返回模板名称
    否则返回None
    """
    template_match = re.search(r'using template (query\d+\.tpl)', sql_content)
    if template_match:
        template_name = template_match.group(1)
        # 去掉.tpl后缀，只保留query名称
        return template_name.replace('.tpl', '')
    return None

def parse_and_generate_out_files(input_file_path, output_dir="."):
    """
    解析指定的std.out输入文件，并为每个查询生成独立的.out文件
    如果一个查询模板只有一个index，则命名为queryname.out
    如果一个查询模板有多个index，则命名为queryname_index.out

    Args:
        input_file_path (str): 输入文件路径
        output_dir (str): 输出目录路径，默认为当前目录
    """

    # 检查输入文件是否存在
    if not os.path.exists(input_file_path):
        raise IOError("输入文件不存在: {}".format(input_file_path))

    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

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

    # 解析所有查询块并提取信息
    query_data = []

    for block in file_blocks:
        # 提取文件名
        filename_match = re.match(r'(.*?)\n', block)
        if not filename_match:
            continue
        filename = filename_match.group(1).strip()

        # 提取SQL内容
        sql_match = re.search(r'SQL:\n(.*?)(?:\n\nfileName:|\Z)', block, re.DOTALL)
        sql_content = sql_match.group(1) if sql_match else ""

        # 提取查询名称（优先使用模板名称）
        query_name = os.path.basename(filename)
        query_name_without_ext = os.path.splitext(query_name)[0]

        # 检查SQL中是否包含模板信息
        template_name = extract_template_name(sql_content)
        if template_name:
            query_name_without_ext = template_name
            display_query_name = template_name  # 用于显示的查询名称
        else:
            display_query_name = query_name_without_ext  # 用于显示的查询名称

        # 提取所有的Index和Result块
        index_blocks = re.split(r'Index\s*:', block)
        # 第一个块是SQL内容，跳过
        for i in range(1, len(index_blocks)):
            index_block = index_blocks[i]

            # 提取Index
            index_lines = index_block.strip().split('\n')
            if not index_lines:
                continue

            index = index_lines[0].strip()

            # 提取Result
            result_match = re.search(r'Result:\n(.*)', index_block, re.DOTALL)
            result = result_match.group(1).strip() if result_match else "N/A"

            # 保存查询数据
            query_data.append({
                'filename': filename,
                'query_name': query_name_without_ext,
                'display_query_name': display_query_name,
                'index': index,
                'result': result
            })

    # 统计每个查询名称的index数量
    query_name_count = defaultdict(int)
    for data in query_data:
        query_name_count[data['query_name']] += 1

    # 生成.out文件
    generated_files = []
    for data in query_data:
        query_name = data['query_name']
        index = data['index']
        filename = data['filename']
        display_query_name = data['display_query_name']

        # 确定输出文件名
        if query_name_count[query_name] == 1:
            # 只有一个index，使用queryname.out格式
            output_filename = "{}.out".format(query_name)
        else:
            # 有多个index，使用queryname_index.out格式
            output_filename = "{}_{}.out".format(query_name, index)

        output_filepath = os.path.join(output_dir, output_filename)

        # 写入.out文件（兼容Python 2和3）
        if sys.version_info[0] == 3:
            # Python 3
            with open(output_filepath, 'w', encoding='utf-8') as f:
                f.write("Query: {}\n".format(display_query_name))
                f.write("Index: {}\n".format(index))
                f.write("Result:\n{}\n".format(data['result']))
        else:
            # Python 2
            import codecs
            with codecs.open(output_filepath, 'w', encoding='utf-8') as f:
                f.write("Query: {}\n".format(display_query_name))
                f.write("Index: {}\n".format(index))
                f.write("Result:\n{}\n".format(data['result']))

        generated_files.append(output_filepath)
        print("Generated: {}".format(output_filepath))

    print("\n总共生成了 {} 个文件".format(len(generated_files)))
    return generated_files

def main():
    """
    主函数，支持命令行参数
    """
    parser = argparse.ArgumentParser(description='解析TPC-DS基准测试输出文件，为每个查询生成独立的.out结果文件')
    parser.add_argument('input_file', help='输入文件路径 (例如: std.out)')
    parser.add_argument('-o', '--output', default='query_results', help='输出目录路径 (默认: query_results)')

    args = parser.parse_args()

    try:
        parse_and_generate_out_files(args.input_file, args.output)
    except Exception as e:
        print("错误: {}".format(e))
        return 1

    return 0

# 使用示例
if __name__ == "__main__":
    # 可以直接调用函数指定参数
    # parse_and_generate_out_files("std.out", "query_results")

    # 或者通过命令行参数
    exit(main())