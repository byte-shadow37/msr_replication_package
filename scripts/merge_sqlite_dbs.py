import sqlite3
import os
from pathlib import Path


def merge_db_files(source_folder, output_db='merged_database.db'):
    """
    将指定文件夹下的所有.db文件合并到一个SQLite数据库中

    参数:
        source_folder: 包含.db文件的文件夹路径
        output_db: 输出的合并数据库文件名
    """

    # 创建或连接到输出数据库
    conn_out = sqlite3.connect(output_db)
    cursor_out = conn_out.cursor()

    # 获取文件夹中的所有.db文件,排除输出文件本身
    output_path = Path(output_db).resolve()
    db_files = [f for f in Path(source_folder).glob('*.db')
                if f.resolve() != output_path]

    if not db_files:
        print(f"在 {source_folder} 中没有找到.db文件")
        conn_out.close()
        return

    print(f"找到 {len(db_files)} 个数据库文件")

    # 遍历每个数据库文件
    for db_file in db_files:
        print(f"\n处理: {db_file.name}")

        # 使用文件名(不含扩展名)作为表名前缀
        prefix = db_file.stem.replace('-', '_').replace(' ', '_')

        try:
            # 连接到源数据库
            conn_src = sqlite3.connect(str(db_file))
            cursor_src = conn_src.cursor()

            # 获取源数据库中的所有表
            cursor_src.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor_src.fetchall()

            # 复制每个表
            for (table_name,) in tables:
                if table_name == 'sqlite_sequence':
                    continue

                # 创建新的表名(前缀_原表名)
                new_table_name = f"{prefix}_{table_name}"

                print(f"  - 复制表: {table_name} -> {new_table_name}")

                # 获取表结构和列信息
                cursor_src.execute(f"PRAGMA table_info({table_name})")
                columns = cursor_src.fetchall()

                # 构建新表的创建语句
                col_defs = []
                for col in columns:
                    col_id, col_name, col_type, not_null, default_val, pk = col
                    col_def = f"{col_name} {col_type}"
                    if not_null:
                        col_def += " NOT NULL"
                    if default_val is not None:
                        col_def += f" DEFAULT {default_val}"
                    if pk:
                        col_def += " PRIMARY KEY"
                    col_defs.append(col_def)

                create_sql = f"CREATE TABLE IF NOT EXISTS {new_table_name} ({', '.join(col_defs)})"

                # 在输出数据库中创建表
                cursor_out.execute(create_sql)

                # 复制数据
                cursor_src.execute(f"SELECT * FROM {table_name}")
                rows = cursor_src.fetchall()

                if rows:
                    # 获取列数
                    num_cols = len(rows[0])
                    placeholders = ','.join(['?' for _ in range(num_cols)])

                    cursor_out.executemany(
                        f"INSERT INTO {new_table_name} VALUES ({placeholders})",
                        rows
                    )
                    print(f"    插入 {len(rows)} 行数据")
                else:
                    print(f"    表为空,未插入数据")

            conn_src.close()

        except Exception as e:
            print(f"处理 {db_file.name} 时出错: {e}")
            continue

    # 提交更改并关闭连接
    conn_out.commit()
    conn_out.close()

    print(f"\n✓ 合并完成! 输出文件: {output_db}")
    print(f"✓ 数据库大小: {os.path.getsize(output_db) / 1024:.2f} KB")


if __name__ == "__main__":
    # 使用示例
    source_folder = "/Users/xingqian/Desktop/MSR_Challenge/"  # 修改为你的文件夹路径
    output_database = "merged_database.db"  # 修改为你想要的输出文件名

    merge_db_files(source_folder, output_database)