import csv
import pymysql
import bson
from datetime import datetime
import time

# 数据库连接配置
#注意这边进行你自己的数据库配置
db_config = {}

csv_file_path = 'chinese_university_rankings_2024.csv'

IMPORT_YEAR = 2024

# 导入批次标识，用于后续可能的回滚操作
IMPORT_BATCH_ID = f"import_chinese_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def main():
    conn = None
    cursor = None
    try:
        # 连接数据库并验证
        print(f"正在连接数据库: {db_config['host']}:{db_config['port']} - {db_config['database']}")
        conn = pymysql.connect(**db_config)
        # 禁用自动提交，确保我们显式控制事务
        conn.autocommit(False)
        cursor = conn.cursor()
        print("数据库连接成功！")

        # 验证连接状态
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"连接验证成功: {result}")

        # 显示当前数据库
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]
        print(f"当前数据库: {current_db}")

        # 检查表是否存在（根据用户提供的数据表格式）
        cursor.execute("SHOW TABLES LIKE 'school_ranking_arwu_chinese'")
        table_exists = cursor.fetchone() is not None
        print(f"表 school_ranking_arwu_chinese 存在: {table_exists}")

        # 读取CSV文件并插入数据，支持多种编码
        # 测试显示CSV文件使用gbk编码
        encodings_to_try = ['gbk', 'utf-8', 'gb2312', 'ansi']
        file_opened = False
        rows_inserted = 0

        # 构建SQL插入语句
        sql = """
        INSERT INTO school_ranking_arwu_chinese (
            id, school_id, ranking, ranking_text, prev_ranking,
            school_cname, school_ename, year_time, update_date_time,
            quality_students, cultivation_results, scientific_research_scale,
            quality_research, top_results, top_talent, technology_services,
            industry_university_research_cooperation, achievement_conversion, overall_score, country_id,
            school_level, subject_level, school_resources, teacher_scale_structure,
            talent_training, scientific_research, serve_society, academic_talent,
            major_projects_achievements, international_competitiveness
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for encoding in encodings_to_try:
            try:
                print(f"尝试使用 {encoding} 编码打开文件...")
                with open(csv_file_path, 'r', encoding=encoding) as csvfile:
                    csv_reader = csv.DictReader(csvfile)
                    file_opened = True
                    print(f"成功使用 {encoding} 编码打开文件")

                    # 使用DictReader读取CSV，这样可以通过列名访问数据
                    csv_reader = csv.DictReader(csvfile)

                    # 获取所有行数据
                    rows = list(csv_reader)
                    print(f"总共读取到 {len(rows)} 行数据")

                    for idx, row in enumerate(rows, 1):
                        print(f"处理行{idx}: 排名={row.get('ranking', 'N/A')}, 学校={row.get('school_cname', 'N/A')}")
                        # 生成ID
                        id = bson.objectid.ObjectId().__str__()

                        try:
                            # 从'ranking'列获取排名
                            ranking = int(row.get('ranking', 0))
                            print(f"成功获取排名: {ranking}")
                        except (ValueError, TypeError) as e:
                            print(f"排名格式错误: {str(e)}，使用默认值{idx}")
                            ranking = idx  # 使用行号作为默认排名

                        # 准备参数
                        try:
                            params = (
                                id,  # id
                                '',  # school_id 空着
                                ranking,  # ranking
                                str(ranking),  # ranking_text - 与ranking保持一致
                                0,  # prev_ranking
                                row.get('school_cname', row.get('cname', '')),
                                row.get('school_ename', row.get('ename', '')),  # school_ename
                                IMPORT_YEAR,  # year_time 2024年
                                datetime.now(),  # update_date_time
                                0, 0, 0, 0, 0, 0, 0, 0, 0,  # 前9个默认值字段
                                float(row.get('overall_score', row.get('total_score', 0)) or 0),  # overall_score
                                '',  # country_id
                                # 新增字段 - 添加空值处理
                                float(row.get('school_level', 0) or 0),
                                float(row.get('subject_level', 0) or 0),
                                float(row.get('school_resources', 0) or 0),
                                float(row.get('teacher_scale_structure', 0) or 0),
                                float(row.get('talent_training', 0) or 0),
                                float(row.get('scientific_research', 0) or 0),
                                float(row.get('serve_society', 0) or 0),
                                float(row.get('academic_talent', 0) or 0),
                                float(row.get('major_projects_achievements', 0) or 0),
                                float(row.get('international_competitiveness', 0) or 0)
                            )
                        except Exception as e:
                            print(f"准备参数时出错: {str(e)}")
                            # 使用默认值继续
                            params = (
                                id, '', ranking, row.get('ranking', row.get('rank', '')), 0,
                                row.get('school_cname', row.get('cname', '')),
                                row.get('school_ename', row.get('ename', '')),
                                IMPORT_YEAR, datetime.now(),
                                0, 0, 0, 0, 0, 0, 0, 0, 0,
                                float(row.get('overall_score', row.get('total_score', 0)) or 0), '',
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                            )

                        # 执行插入
                        try:
                            affected_rows = cursor.execute(sql, params)
                            rows_inserted += affected_rows
                        except pymysql.MySQLError as e:
                            print(f"插入数据失败: 错误: {e.args[0]}, {e.args[1]}")
                            print(f"当前处理的数据: {row}")

                        # 每50行提交一次
                        if rows_inserted % 50 == 0:
                            conn.commit()
                            print(f"[{time.strftime('%H:%M:%S')}] 已成功插入并提交 {rows_inserted} 行数据")

                # 成功打开文件后跳出循环
                break
            except UnicodeDecodeError:
                print(f"{encoding} 编码解码失败，尝试下一个编码...")
                continue
            except Exception as e:
                print(f"打开文件时出错: {str(e)}")
                continue

        if not file_opened:
            raise Exception("无法打开CSV文件，请检查文件编码格式")

        # 提交剩余数据
        try:
            conn.commit()
            print(f"[{time.strftime('%H:%M:%S')}] 最终提交完成")
        except pymysql.MySQLError as e:
            print(f"最终提交失败: {e.args[0]}, {e.args[1]}")
            if conn:
                try:
                    conn.rollback()
                    print("执行回滚操作...")
                except:
                    print("回滚操作失败")

        # 最终验证数据是否真正插入
        cursor.execute("SELECT COUNT(*) FROM school_ranking_arwu_chinese WHERE year_time = %s", (IMPORT_YEAR,))
        final_count = cursor.fetchone()[0]
        print(f"[{time.strftime('%H:%M:%S')}] 数据导入完成，程序报告插入 {rows_inserted} 行数据")
        print(f"[{time.strftime('%H:%M:%S')}] 数据库中实际存在 {final_count} 条本批次数据")

        if final_count > 0:
            print("数据已成功写入数据库！")
        else:
            print("警告：数据库中未找到插入的数据！")

    except pymysql.MySQLError as e:
        print(f"MySQL错误: {e.args[0]}, {e.args[1]}")
        if conn:
            print("执行回滚操作...")
            conn.rollback()
    except Exception as e:
        print(f"发生未知错误: {str(e)}")
        if conn:
            print("执行回滚操作...")
            conn.rollback()

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("数据库连接已关闭")


if __name__ == "__main__":
    main()