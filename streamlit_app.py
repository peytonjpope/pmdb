import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os
import re

load_dotenv()

# Functions
def letterboxd_slug(title):
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', '', title)
    title = title.strip().replace(' ', '-')
    return title

def percentile_label(p):
    p = max(1, p)
    if p == 1: suffix = "st"
    elif p == 2: suffix = "nd"
    elif p == 3: suffix = "rd"
    else: suffix = "th"
    return f"{p}{suffix} percentile"

def percentile_color(p):
    if p >= 90: return "normal"      # green
    elif p <= 50: return "inverse"   # red
    else: return "off"               # grey

def diff_color(diff):
    if diff > 0.3: return "green"
    elif diff < -0.3: return "red"
    else: return "gray"

# Snowflake connection parameters
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'P_MOVIE_DB'
SCHEMA = 'BRONZE'

if 'initialized' not in st.session_state:
    st.session_state.conn = None
    st.session_state.df = None
    

if st.session_state.conn is None:
    # Connect to Snowflake
    print("Connecting to Snowflake...")
    st.session_state.conn = snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    
conn = st.session_state.conn

# Pull data
if st.session_state.df is None:
    st.session_state.df = pd.read_sql("""
        SELECT * FROM P_MOVIE_DB.DBT_GOLD.JOINED_MOVIE_RATINGS
    """, conn)

df = st.session_state.df
    

st.set_page_config(page_title="PMDb", page_icon="🎞️", layout="wide")
st.title("🎞️ PMDb")
st.markdown("Comparing **IMDb** and **Letterboxd** ratings across 37,000+ films — where they agree, and where they don't.")
st.divider()



graph_tab, rank_tab, film_tab = st.tabs(["Graphs", "Rankings", "Film Details"])

with graph_tab:
        
    min_votes = st.slider("Minimum Votes", 1000, 500000, 10000)

    filtered = df[df['NUM_VOTES'] > min_votes]

    fig = px.scatter(
        filtered,
        x='IMDB_RATING',
        y='LB_RATING',
        hover_name='TITLE',
        color='RAW_RATING_DIFF',
        color_continuous_scale='BrBG',
        title='IMDb vs Letterboxd Rating Comparison',
        labels={
            'IMDB_RATING': 'IMDb Rating (out of 10)',
            'LB_RATING': 'Letterboxd Rating (out of 5)',
            'RAW_RATING_DIFF': 'Rating Difference'
        }
    )

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font_color='white',
        coloraxis_colorbar_title='Platform Lean'
    )

    fig.update_traces(
        marker=dict(size=5, opacity=0.6)
    )

    st.plotly_chart(fig, use_container_width=True)
    
    
    
    st.divider()
    st.subheader("Biggest Disagreements")

    top_n = st.slider("Number of Films", 10, 50, 20, key="diverge_n")
    min_votes_div = st.slider("Minimum Votes", 1000, 500000, 50000, key="diverge_votes")

    div_df = df[df['NUM_VOTES'] > min_votes_div].copy()
    div_df = div_df.nlargest(top_n, 'ABS_RATING_DIFF')
    div_df = div_df.sort_values('RAW_RATING_DIFF')

    div_df['COLOR'] = div_df['RAW_RATING_DIFF'].apply(
        lambda x: 'Letterboxd Favors' if x > 0 else 'IMDb Favors'
    )

    fig2 = px.bar(
        div_df,
        x='RAW_RATING_DIFF',
        y='TITLE',
        orientation='h',
        color='COLOR',
        color_discrete_map={
            'Letterboxd Favors': '#44a299',
            'IMDb Favors': '#debf7a'
        },
        title='Biggest Rating Disagreements',
        labels={
            'RAW_RATING_DIFF': 'Rating Difference',
            'TITLE': '',
            'COLOR': ''
        },
        hover_data=['IMDB_RATING', 'LB_RATING', 'NUM_VOTES']
    )

    fig2.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font_color='white',
        xaxis=dict(zeroline=True, zerolinecolor='white', zerolinewidth=1),
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )

    st.plotly_chart(fig2, use_container_width=True)




with rank_tab:
    
    rc1, rc2, rc3 = st.columns(3)
    
    with rc1:
        sort_by = st.selectbox("Metric", [
            "Combined Rating",
            "IMDb Rating",
            "Letterboxd Rating",
            "Largest Lean (IMDb)",
            "Largest Lean (Letterboxd)"
        ])
    
    with rc2:
        decade_options = ["All"] + sorted(list(set((df['RELEASE_YEAR'] // 10 * 10).dropna().astype(int).tolist())))
        decade = st.selectbox("Decade", decade_options)
    
    with rc3:
        min_votes_rank = st.slider("Minimum Votes", 1000, 500000, 10000, key="rank_votes")

    sort_col_map = {
        "Combined Rating": "COMPOSITE_RATING",
        "IMDb Rating": "IMDB_RATING",
        "Letterboxd Rating": "LB_RATING",
        "Largest Lean (IMDb)": "RAW_RATING_DIFF",
        "Largest Lean (Letterboxd)": "RAW_RATING_DIFF"
    }

    rank_df = df[df['NUM_VOTES'] > min_votes_rank].copy()
    
    if decade != "All":
        rank_df = rank_df[(rank_df['RELEASE_YEAR'] >= decade) & (rank_df['RELEASE_YEAR'] < decade + 10)]

    ascending = sort_by == "Largest Lean (IMDb)"
    rank_df = rank_df.sort_values(sort_col_map[sort_by], ascending=ascending).head(1000).reset_index(drop=True)
    rank_df.index += 1

    display_df = pd.DataFrame({
        "#": rank_df.index,
        "Film": rank_df['TITLE'] + " (" + rank_df['RELEASE_YEAR'].astype(str) + ")",
        "Combined": rank_df['COMPOSITE_RATING'].round(2),
        "IMDb": rank_df['IMDB_RATING'],
        "Letterboxd": rank_df['LB_RATING'],
        "Lean": rank_df['RAW_RATING_DIFF'].round(2),
        "Votes": rank_df['NUM_VOTES']
    })

    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Votes": st.column_config.NumberColumn(format="%d"),
            "Lean": st.column_config.NumberColumn(format="%.2f"),
            "#": st.column_config.NumberColumn(width="small")
        }
    )
    

with film_tab:
    
    selected_film = st.selectbox(
        "Search a Film",
        options=list(df.itertuples()), 
        format_func=lambda f: f"{f.TITLE} ({f.RELEASE_YEAR})"
    )

    if selected_film:
        
        col1, col2 = st.columns([1, 2])

        with col1:
            st.image(selected_film.POSTER_LINK, width=220)

        with col2:
            st.subheader(f"{selected_film.TITLE}")
            st.caption(f"{selected_film.RELEASE_YEAR} · {int(selected_film.RUNTIME_MIN)} min · {selected_film.NUM_VOTES:,} votes · [IMDb](https://www.imdb.com/title/{selected_film.TT_ID}) · [Letterboxd](https://letterboxd.com/film/{letterboxd_slug(selected_film.TITLE)}/)")
            
            composite_percentile = max(1, round((df['COMPOSITE_RATING'] < selected_film.COMPOSITE_RATING).mean() * 100))
            imdb_percentile = max(1, round((df['IMDB_RATING'] < selected_film.IMDB_RATING).mean() * 100))
            lb_percentile = max(1, round((df['LB_RATING'] < selected_film.LB_RATING).mean() * 100))

            m1, m2, m3 = st.columns(3)
            m1.metric("Combined", round(selected_film.COMPOSITE_RATING, 2),
                delta=percentile_label(composite_percentile),
                delta_color=percentile_color(composite_percentile),
                delta_arrow="off")
            m2.metric("IMDb", selected_film.IMDB_RATING,
                delta=percentile_label(imdb_percentile),
                delta_color=percentile_color(imdb_percentile),
                delta_arrow="off")
            m3.metric("Letterboxd", selected_film.LB_RATING,
                delta=percentile_label(lb_percentile),
                delta_color=percentile_color(lb_percentile),
                delta_arrow="off")


            st.divider()

            diff = round(selected_film.RAW_RATING_DIFF, 2)
            abs_diff = abs(diff)

            if abs_diff < 0.1:
                st.info(f"Both platforms rate this film similarly")
            elif abs_diff < 0.3:
                if diff > 0:
                    st.success(f"Letterboxd slightly favors this film by **{abs_diff}** points")
                else:
                    st.warning(f"IMDb slightly favors this film by **{abs_diff}** points")
            elif diff > 0:
                st.success(f"Letterboxd favors this film by **{abs_diff}** points")
            else:
                st.warning(f"IMDb favors this film by **{abs_diff}** points")


            
        