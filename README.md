# Netflix Data Analytics with dbt

A comprehensive dbt project for analyzing Netflix/MovieLens data, demonstrating modern data engineering best practices and advanced analytics capabilities.

## 🎯 Project Overview

This project transforms raw MovieLens data into a comprehensive analytics platform, enabling:
- **User Behavior Analysis** - Understanding user preferences and rating patterns
- **Content Performance Analytics** - Movie popularity and rating analysis
- **Recommendation Engine Support** - Data foundation for ML recommendation systems
- **Business Intelligence** - Executive dashboards and strategic insights

## 🏗️ Architecture

### Data Flow
```
Raw Data (Bronze) → Staging (Silver) → Data Warehouse (Gold) → Business Marts
```

### Model Layers
- **Staging**: Raw data cleansing and standardization
- **Dimensions**: Master data for movies, users, and tags
- **Facts**: Transactional data for ratings and scores
- **Marts**: Business-ready analytics tables

## 📊 Key Features

### Data Quality & Testing
- ✅ Comprehensive data quality tests
- ✅ Business logic validation
- ✅ Referential integrity checks
- ✅ Custom test macros

### Performance Optimizations
- 🚀 Incremental models for large fact tables
- 🚀 Clustering strategies for query performance
- 🚀 Materialization strategies optimized per layer
- 🚀 Pre/post hooks for monitoring

### Advanced Analytics
- 📈 User behavior segmentation
- 📈 Movie performance categorization
- 📈 Genre analysis and insights
- 📈 Recommendation system support

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- dbt Core 1.0+
- Snowflake account (or compatible data warehouse)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd dbt-databuildtool--masterclass-netflix-project

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install dbt-core dbt-snowflake

# Install dbt packages
cd netflix
dbt deps
```

### Configuration
1. Set up your `profiles.yml` with Snowflake credentials
2. Configure your `dbt_project.yml` settings
3. Run the project:

```bash
# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📁 Project Structure

```
netflix/
├── models/
│   ├── staging/          # Raw data cleansing
│   ├── dim/             # Dimension tables
│   ├── fct/             # Fact tables
│   ├── mart/            # Business marts
│   └── schema.yml       # Model documentation
├── macros/              # Reusable SQL logic
├── tests/               # Custom data quality tests
├── snapshots/           # Slowly changing dimensions
├── seeds/               # Reference data
├── analyses/            # Ad-hoc analysis queries
└── exposures.yml        # Business applications
```

## 🔍 Key Models

### Core Dimensions
- **`dim_movies`** - Movie metadata with genre analysis
- **`dim_users`** - User master data
- **`dim_genome_tags`** - Content categorization tags

### Fact Tables
- **`fct_ratings`** - User ratings (incremental)
- **`fct_genome_scores`** - Movie-tag relevance scores

### Business Marts
- **`mart_user_behavior`** - User segmentation and preferences
- **`mart_movie_performance`** - Movie analytics and categorization
- **`mart_movie_releases`** - Ratings with release date context

## 🧪 Data Quality

The project includes comprehensive testing:
- **Uniqueness tests** on primary keys
- **Not null tests** on critical fields
- **Referential integrity** between tables
- **Business logic validation** (rating ranges, etc.)
- **Custom tests** for data completeness

## 📈 Analytics Capabilities

### User Analytics
- User segmentation (Heavy, Regular, Light, Casual)
- Rating behavior analysis
- Genre preference tracking
- Activity pattern analysis

### Content Analytics
- Movie performance categorization
- Rating consistency analysis
- Genre popularity trends
- Content recommendation insights

### Business Intelligence
- Executive dashboards
- Content strategy reports
- Recommendation engine support
- A/B testing data foundation

## 🔧 Advanced Features

### Macros
- Data quality validation functions
- Surrogate key generation
- Audit column creation
- Performance monitoring hooks

### Snapshots
- Slowly changing dimension tracking
- Historical data preservation
- Change detection and monitoring

### Exposures
- Dashboard dependencies
- API endpoint tracking
- Business application mapping

## 📊 Sample Queries

### Top Rated Movies
```sql
SELECT movie_title, avg_rating, total_ratings
FROM {{ ref('mart_movie_performance') }}
WHERE movie_category = 'Blockbuster'
ORDER BY avg_rating DESC
LIMIT 10;
```

### User Segmentation
```sql
SELECT user_segment, COUNT(*) as user_count
FROM {{ ref('mart_user_behavior') }}
GROUP BY user_segment;
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- MovieLens dataset for providing the foundation data
- dbt Labs for the excellent data transformation framework
- Netflix for inspiration on data-driven content strategy
