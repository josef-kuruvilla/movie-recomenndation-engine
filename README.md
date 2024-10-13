# Movie Recommendation System

## Overview
This project implements a movie recommendation system using Databricks, featuring a structured approach with bronze, silver, and gold data layers. It also integrates IMDb data and provides visualizations for Netflix data analytics.

## Table of Contents
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [Getting Started](#getting-started)
- [Tables Used](#tables-used)
- [Notebooks](#notebooks)
- [Visualizations](#visualizations)
- [Contributing](#contributing)
- [License](#license)

## Project Structure
This project follows a layered architecture:
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Data cleaning and transformation
- **Gold Layer**: Aggregated and enriched data for analysis and recommendations

## Technologies Used
- [Databricks](https://databricks.com/)
- Apache Spark
- Python (Machine learning library used)
- SQL
- Pandas
- Databricks visualization (for visualizations)

## Getting Started

### Prerequisites
- Databricks account
- Access to a Databricks workspace

### Setup
1. Clone this repository or import it directly into your Databricks workspace.
2. Create a new cluster in Databricks and attach it to the notebooks.
3. Install any required libraries if they are not already available in your environment.

## Tables Used
- **Movies**: Contains movie details including titles and genres.
- **Ratings**: User ratings for movies, sourced from IMDb.
- **User Data**: User profiles and interactions.

## Notebooks
1. **Bronze Layer Notebook**: Handles raw data ingestion.
2. **Silver Layer Notebook**: Cleans and transforms the data for analysis.
3. **Gold Layer Notebook**: Aggregates data for insights and recommendations.
4. **Add IMDb ID Notebook**: Enriches the dataset by adding IMDb IDs to movies.
5. **Add IMDb Movie User Ratings Notebook**: Integrates user ratings from IMDb for enhanced recommendations.

## Visualizations
- Interactive visualizations created for Netflix data analytics to explore trends and insights in movie ratings and user preferences.

## Contributing
Contributions are welcome! Please feel free to open issues or submit pull requests for improvements and enhancements.


---

For any questions or suggestions, feel free to reach out!
