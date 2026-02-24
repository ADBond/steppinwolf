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

hist = alt.Chart(data).mark_bar().encode(
    alt.X("raw_count:Q").bin(step=1000),
    y="count()",
).save("hist.html")

tab = con.read_csv("data/processed/excess.csv")
raw_values = tab.select("units", "tens", "hundreds", "thousands").fetchall()

data = alt.Data(
    values=[
        {"units": units, "tens": tens, "hundreds": hundreds, "thousands": thousands}
        for units, tens, hundreds, thousands in raw_values
    ]
)

stacked_hist = alt.HConcatChart(
    hconcat=[
        alt.VConcatChart(
            vconcat=[
                alt.Chart(data).mark_bar().encode(x="units:O", y="count()"),
                alt.Chart(data).mark_bar().encode(x="tens:O", y="count()"),
                alt.Chart(data).mark_bar().encode(x="hundreds:O", y="count()"),
            ]
        ),
        alt.Chart(data).mark_bar().encode(x="thousands:O", y="count()")
    ]
).save("stacked_hist.html")
