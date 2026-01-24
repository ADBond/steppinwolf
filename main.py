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
            FLOOR(
                AVG(raw_count) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN 28 PRECEDING AND CURRENT ROW
                )
            )::INTEGER AS monthly_rolling_avg,
            FLOOR(
                AVG(raw_count) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN 84 PRECEDING AND CURRENT ROW
                )
            )::INTEGER AS quarterly_rolling_avg,
            notes,
        FROM
            raw
        ORDER BY
            day_num
        """
    )
    con.table("enhanced").to_csv("data/processed/enhanced.csv")

    con.execute(
        """
        CREATE TABLE summary_dow AS
        SELECT
            -- is there something in-built for this?
            CASE
                WHEN day_of_week = 'Monday' THEN 1
                WHEN day_of_week = 'Tuesday' THEN 2
                WHEN day_of_week = 'Wednesday' THEN 3
                WHEN day_of_week = 'Thursday' THEN 4
                WHEN day_of_week = 'Friday' THEN 5
                WHEN day_of_week = 'Saturday' THEN 6
                WHEN day_of_week = 'Sunday' THEN 7
                ELSE -1
            END AS day_index,
            day_of_week,
            AVG(raw_count) AS avg_count,
            STDDEV_SAMP(raw_count) AS std_count,
        FROM
            enhanced
        GROUP BY
            day_of_week
        ORDER BY
            day_index
        """
    )
    con.table("summary_dow").to_csv("data/processed/summary_dow.csv")

    con.sql("SELECT sum(raw_count) AS total FROM raw").show()


if __name__ == "__main__":
    main()
