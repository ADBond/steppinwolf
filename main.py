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
            FLOOR(
                AVG(raw_count) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            )::INTEGER AS avg_to_date,
            notes,
        FROM
            raw
        ORDER BY
            day_num
        """
    )
    con.table("enhanced").to_csv("data/processed/enhanced.csv")


if __name__ == "__main__":
    main()
