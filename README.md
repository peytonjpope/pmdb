# 🎞️ [PMDb](https://pmoviedb.streamlit.app)

*Interactive web-app showcasing Snowflake database of 37,000+ films with IMDb and Letterboxd ratings*

### Technical Architecture

- **Ingestion**: Python (pandas, Snowflake Connector) loading several kaggle datasets
- **Storage**: Snowflake with Bronze → Silver → Gold medallion architecture
- **Transformation**: dbt models cleaning and joining across multiple source tables
- **Visualization**: Streamlit + Plotly interactive web app

(more documentation [here](pmdb_proj/README.md))