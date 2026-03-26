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

bar = alt.Chart(data).mark_bar(color="#dddddd").encode(
    x="day_index:O",
    y="raw_count:Q"
)

# TODO: check frame bounds logic
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
quarterly_line = alt.Chart(data).mark_line(color="#008800").transform_window(
    monthly_rolling_avg="mean(raw_count)",
    frame=[-84, 0]
).encode(
    x="day_index:O",
    y="quarterly_rolling_avg:Q"
)
overall_line = alt.Chart(data).mark_line(color="black").transform_window(
    to_date_avg="mean(raw_count)",
    frame=[None, 0]
).encode(
    x="day_index:O",
    y="to_date_avg:Q"
)
thresh_line = alt.Chart().mark_rule(color="#006600", strokeDash=(8, 8)).encode(
    y=alt.datum(10_000),
)

(bar + overall_line + monthly_line + weekly_line + quarterly_line + thresh_line).properties(width=600).save("bar.html")

hist = alt.Chart(data).mark_bar().encode(
    alt.X("raw_count:Q").bin(step=1000),
    y="count()",
).save("hist.html")

raw_values = tab.select("day_of_week", "raw_count").fetchall()
data = alt.Data(
    values=[
        {"dow": day_of_week, "raw_count": raw_count}
        for day_of_week, raw_count in raw_values
    ]
)
scatter = alt.Chart(data).mark_circle(size=60).encode(
    x="dow:N",
    y="raw_count:Q",
    xOffset="jitter:Q",
    color="dow:N"
).transform_calculate(
    # Generate Gaussian jitter with a Box-Muller transform
    jitter="sqrt(-2*log(random()))*cos(2*PI*random())"
).properties(width=200).save("scatter.html")


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
