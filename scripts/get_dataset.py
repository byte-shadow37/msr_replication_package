from datasets import load_dataset


def get_data(table_name):
    dataset = load_dataset("hao-li/AIDev", table_name)

    dataset['train'].to_csv(f"{table_name}.csv", index=False)

    import sqlite3
    conn = sqlite3.connect(f"{table_name}.db")
    dataset['train'].to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()


table = ""
get_data(table)