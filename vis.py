import altair as alt
import duckdb

con = duckdb.connect()

tab = con.read_csv("data/processed/enhanced.csv")
raw_values = tab.select("day_num", "raw_count").fetchall()
data = alt.Data(
    values=[
        {"day_index": day_index, "raw_count": raw_count}
        for day_index, raw_count in raw_values
    ]
)

bar = alt.Chart(data).mark_bar(color="grey").encode(
    x="day_index:O",
    y="raw_count:Q"
)

weekly_line = alt.Chart(data).mark_line(color="red").transform_window(
    weekly_rolling_avg="mean(raw_count)",
    frame=[-7, 0]
).encode(
    x="day_index:O",
    y="weekly_rolling_avg:Q"
)
monthly_line = alt.Chart(data).mark_line(color="blue").transform_window(
    monthly_rolling_avg="mean(raw_count)",
    frame=[-28, 0]
).encode(
    x="day_index:O",
    y="monthly_rolling_avg:Q"
)


(bar + weekly_line + monthly_line).properties(width=600).save("bar.html")
