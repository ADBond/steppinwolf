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
                MEDIAN(raw_count) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            )::INTEGER AS median_to_date,
            -- equivalent to days over or at 10000 - days under
            2 * COUNTIF(raw_count >= 10000) OVER (
                ORDER BY day_num
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - day_num AS net_days_over_ten,
            -ROUND(
                COUNTIF(raw_count <= 5000) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) - day_num/4,
                2
            ) AS net_lower,
            ROUND(
                COUNTIF(raw_count >= 12500) OVER (
                    ORDER BY day_num
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) - day_num/4,
                2
            ) AS net_upper,
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
            COUNT(*) AS occurences,
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
            round(AVG(raw_count), 2) AS avg_count,
            round(STDDEV_SAMP(raw_count), 2) AS std_count,
            round(MEDIAN(raw_count)) AS med_count,
        FROM
            enhanced
        GROUP BY
            day_of_week
        ORDER BY
            day_index
        """
    )
    con.table("summary_dow").to_csv("data/processed/summary_dow.csv")

    con.execute(
        """
        CREATE TABLE goal_prog AS
        SELECT
            count(*) AS days_completed,
            365 - days_completed AS days_remaining,
            sum(raw_count) AS total,
            AVG(raw_count) AS mean_count,
            MEDIAN(raw_count) AS median_count,
            MAX(raw_count) AS max_count,
            quantile_cont(raw_count, 0.25) AS first_quartile_count,
            quantile_cont(raw_count, 0.75) AS third_quartile_count,
            MAX(weekly_rolling_avg) AS highest_weekly_rolling_avg,
            MAX(monthly_rolling_avg) AS highest_monthly_rolling_avg,
        FROM
            enhanced
        """
    )
    con.execute(
        """
        CREATE TABLE goal_prog_long AS
        UNPIVOT goal_prog
        ON *
        INTO
            NAME stat
            VALUE value;
        """
    )
    con.table("goal_prog_long").to_csv("data/processed/goal_progress.csv")

    con.execute(
        """
        CREATE TABLE excess AS
        SELECT
            raw_count % 10 AS units,
            (raw_count % 100 - units) // 10 AS tens,
            (raw_count % 1000 - tens - units) // 100 AS hundreds,
            (raw_count - hundreds - tens - units) // 1000 AS thousands,
        FROM
            enhanced
        """
    )
    con.table("excess").to_csv("data/processed/excess.csv")

    con.sql(
        """
        SELECT
            -- row_number() over () AS row_number,
            raw_count,
            date,
            day_of_week,
        FROM (
            SELECT *
            FROM
                enhanced
            ORDER BY
                raw_count, date
        )
        """
    ).to_csv("data/processed/just_counts.csv")

    con.sql("SELECT sum(raw_count) AS total FROM raw").show()


if __name__ == "__main__":
    main()
