import duckdb

def main():
    con = duckdb.connect()
    con.execute("CREATE TABLE raw AS SELECT * FROM read_csv_auto('data/raw/count.csv')")

    con.execute(
        """
        CREATE TABLE enhanced AS
        SELECT
            day_num,
            DATE '2026-01-01' + day_num::INTEGER - 1 AS date,
            raw_count,
            notes,
        FROM
            raw
        """
    )
    con.table("enhanced").to_csv("data/processed/enhanced.csv")


if __name__ == "__main__":
    main()
