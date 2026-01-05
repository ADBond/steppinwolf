import duckdb

def main():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE raw AS
        SELECT
            day_num,
            raw_count,
            notes,
        FROM read_csv(
            'data/raw/count.csv',
            delim = ',',
            header = true,
            columns = {
                'day_num': 'INT8',
                'raw_count': 'INT16',
                'notes': 'VARCHAR',
            }
        )
        """
    )

    con.execute(
        """
        CREATE TABLE enhanced AS
        SELECT
            day_num,
            DATE '2026-01-01' + day_num::INTEGER - 1 AS date,
            dayname(date) AS day_of_week,
            raw_count,
            SUM(raw_count) OVER (
                ORDER BY day_num
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - 10000*day_num AS eod_balance,
            FLOOR(
                AVG(raw_count) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            )::INTEGER AS avg_to_date,
            FLOOR(
                AVG(raw_count) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
                )
            )::INTEGER AS weekly_rolling_avg,
            SUM(raw_count) OVER (
                ORDER BY day_num
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - 12500*day_num AS ambitious_balance,
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
