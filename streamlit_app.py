import streamlit as st
<<<<<<< HEAD
import snowflake.connector
=======
from sqlalchemy import create_engine
>>>>>>> clean_main
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os
import re

load_dotenv()

# Functions
<<<<<<< HEAD
=======
def highlight_lean_row(row):
    val = row['Lean']
    
    # scale alpha (adjust divisor to control intensity)
    alpha = min(abs(val) / 4, 1)

    if val > 0:
        color = f'background-color: rgba(68, 162, 153, {alpha})'  # Letterboxd
    elif val < 0:
        color = f'background-color: rgba(222, 191, 122, {alpha})'  # IMDb
    else:
        color = ''

    styles = [''] * len(row)
    lean_idx = row.index.get_loc('Lean')
    styles[lean_idx] = color
    
    return styles


>>>>>>> clean_main
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
<<<<<<< HEAD

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
=======
    
# Get secrets
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')

@st.cache_resource
def get_engine():
    return create_engine(
        f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/P_MOVIE_DB/DBT_GOLD?warehouse=COMPUTE_WH'
    )

@st.cache_data(ttl=3600)
def load_data():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM P_MOVIE_DB.DBT_GOLD.JOINED_MOVIE_RATINGS", engine)

df = load_data()
df.columns = df.columns.str.upper()
    

st.set_page_config(page_title="PMDb", page_icon="🎞️", layout="centered")
st.title("🎞️ PMDb")
st.markdown("Database of 37,000+ films with **IMDb** and **Letterboxd** ratings and information. Explore how they agree, disgree, and more...")
# st.divider()


graph_tab, rank_tab, film_tab = st.tabs(["Visualizations", "Rankings", "Film Details"])

with graph_tab:
    st.subheader("Scatterplot Comparison")
        
    gc1, gc2, gc3 = st.columns(3)
    
    with gc1:
        
        min_rating = st.selectbox("Minimum Combined Rating", [
            "Any", "9+", "8+", "7+", "6+", "5+"
        ], key="graph_rating")
        
    with gc2:
        decade_options_graph = ["Any"] + sorted(list(set((df['RELEASE_DECADE']).dropna().astype(int).tolist())), reverse=True)
        decade_graph = st.selectbox("Decade", decade_options_graph, key="graph_decade")
        
    with gc3:

        min_votes = st.slider("Minimum Votes", 1000, 500000, 50000)

    rating_map = {"Any": 0, "9+": 9, "8+": 8, "7+": 7, "6+": 6, "5+": 5}

    filtered = df[df['NUM_VOTES'] > min_votes]
    if min_rating != "Any":
        filtered = filtered[filtered['COMPOSITE_RATING'] >= rating_map[min_rating]]
    if decade_graph != "Any":
        filtered = filtered[(filtered['RELEASE_YEAR'] >= decade_graph) & (filtered['RELEASE_YEAR'] < decade_graph + 10)]
>>>>>>> clean_main

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
    
    
<<<<<<< HEAD
    
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
=======
    st.divider()
    st.subheader("Most Significant Differences")

    dc1, dc2, dc3 = st.columns(3)

    with dc1:
        min_rating_div = st.selectbox("Minimum Combined Rating", [
            "Any", "9+", "8+", "7+", "6+", "5+"
        ], key="div_rating")

    with dc2:
        decade_options_div = ["Any"] + sorted(list(set((df['RELEASE_DECADE']).dropna().astype(int).tolist())), reverse=True)
        decade_div = st.selectbox("Decade", decade_options_div, key="diverge_decade")

    with dc3:
        min_votes_div = st.slider("Minimum Votes", 1000, 500000, 50000, key="diverge_votes")

    div_df = df[df['NUM_VOTES'] > min_votes_div].copy()
    if min_rating_div != "Any":
        div_df = div_df[div_df['COMPOSITE_RATING'] >= rating_map[min_rating_div]]
    if decade_div != "Any":
        div_df = div_df[(div_df['RELEASE_YEAR'] >= decade_div) & (div_df['RELEASE_YEAR'] < decade_div + 10)]

    top_lb = div_df.nlargest(5, 'RAW_RATING_DIFF')
    top_imdb = div_df.nsmallest(5, 'RAW_RATING_DIFF')
    div_df = pd.concat([top_imdb, top_lb]).sort_values('RAW_RATING_DIFF')
    div_df['FAVOR'] = div_df['RAW_RATING_DIFF'].apply(
    lambda x: 'Letterboxd Favors' if x > 0 else 'IMDb Favors'
)
>>>>>>> clean_main

    fig2 = px.bar(
        div_df,
        x='RAW_RATING_DIFF',
        y='TITLE',
        orientation='h',
<<<<<<< HEAD
        color='COLOR',
=======
        color='FAVOR',
>>>>>>> clean_main
        color_discrete_map={
            'Letterboxd Favors': '#44a299',
            'IMDb Favors': '#debf7a'
        },
<<<<<<< HEAD
        title='Biggest Rating Disagreements',
        labels={
            'RAW_RATING_DIFF': 'Rating Difference',
            'TITLE': '',
            'COLOR': ''
=======
        title='Platform Rating Divergence (top 5 each)',
        labels={
            'RAW_RATING_DIFF': 'Rating Difference',
            'TITLE': '',
            'FAVOR': 'Lean'
            
>>>>>>> clean_main
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
<<<<<<< HEAD
=======
    st.subheader("Leaderboards")
>>>>>>> clean_main
    
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
<<<<<<< HEAD
        decade_options = ["All"] + sorted(list(set((df['RELEASE_YEAR'] // 10 * 10).dropna().astype(int).tolist())))
        decade = st.selectbox("Decade", decade_options)
    
    with rc3:
        min_votes_rank = st.slider("Minimum Votes", 1000, 500000, 10000, key="rank_votes")
=======
        decade_options = ["All"] + sorted(list(set((df['RELEASE_YEAR'] // 10 * 10).dropna().astype(int).tolist())), reverse=True)

        decade = st.selectbox("Decade", decade_options)
    
    with rc3:
        min_votes_rank = st.slider("Minimum Votes", 1000, 500000, 50000, key="rank_votes")
>>>>>>> clean_main

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
<<<<<<< HEAD

    st.dataframe(
        display_df,
=======
    
    styled_df = (
        display_df.style
        .apply(highlight_lean_row, axis=1)
        .format({
            "Combined": "{:.2f}",
            "IMDb": "{:.1f}",
            "Letterboxd": "{:.1f}",
            "Lean": "{:.2f}",
            "Votes": "{:,.0f}"
        })
    )

    st.dataframe(
        styled_df,
>>>>>>> clean_main
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

<<<<<<< HEAD
=======
            st.write("")
            
>>>>>>> clean_main
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


<<<<<<< HEAD
            st.divider()
=======
            # st.divider()
>>>>>>> clean_main

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


<<<<<<< HEAD
            
        
=======
st.divider()
st.caption("*Click [here](https://peytonjpope.com/projects/pmdb/) for more info*")
>>>>>>> clean_main
