
# Customer Segmentation using Yelp Reviews Data
### Project Overview
### This project focuses on developing a robust customer segmentation framework using Yelp's extensive reviews database. It involves processing raw data, implementing real-time data streaming, executing sentiment analysis, and aggregating various features to create a multidimensional customer dataset.
 
 
 # Key Features : 
 
   ### Segmentation Data Cube
    Utilized the Yelp Review Dataset to build a data cube that includes both behavioral and predictive attributes of customers.
    This segmentation helps in categorizing customers based on their interactions and preferences.
    
   ### Real-Time Streaming Data Pipeline
    Developed a data pipeline using Apache Kafka to incorporate real-time Yelp reviews.
    This enhancement significantly improves the accuracy and relevance of customer segmentation.
    
   ### Aspect-Based Sentiment Analysis
    Implemented Aspect-Based Sentiment Analysis using TF-IDF (Term Frequency-Inverse Document Frequency) and LDA (Latent Dirichlet Allocation).
    This approach identifies key highlights in reviews, such as mentions of specific food items, services, and ambiance.
    It provides deeper insights into customer preferences, aiding in more granular and effective audience targeting.
    
   ### Integration of Machine Learning Models
    Trained Machine Learning models on a dataset of 3,000 manually labeled Yelp reviews, focusing on aspect-based sentiments.
    Integrated these models into our data cube to enhance segmentation capabilities.
    Utilized LDA for topic extraction, enabling the dataset to more accurately segment customer sentiments on critical aspects like service quality and ambiance in new reviews.
   
   
# How It Helps Businesses
    This project offers businesses a powerful tool to understand and segment their customer base using real-world data from Yelp reviews. By leveraging real-time data and advanced sentiment analysis, businesses can gain actionable insights into customer preferences and behavior, leading to more informed decision-making and personalized 
    customer engagement strategies.


# System Architecture and Workflow

<img width="1343" alt="Screenshot 2024-01-27 at 3 56 12 PM" src="https://github.com/ankur1508ojha/yelp-customer-segmentation/assets/102976689/3858e74d-3c62-4345-8952-3b44b6800321">

 ###  Initial Data Setup
    Data is sourced from Yelp's dataset, which includes users, businesses, reviews, check-ins, and tips.
 ###   Data Processing and Transformation
    The raw data undergoes extraction and transformation to derive meaningful insights and features.

    
 ###    Real-Time Data Streaming
    Kafka is used to ingest real-time data, ensuring the dataset is dynamically updated.
    
 ###    Sentiment Analysis
    Reviews are analyzed to extract sentiment and key phrases, providing deeper insight into customer opinions.
    
 ###    Data Aggregation
    All extracted features are aggregated to form a comprehensive view of each customer.


# Detailed Steps
 ### Step 1  Data Processing (data_processing.py)
    User and Business Data Loading
    Load and process registered users and business entities.
    Partial loading of streaming datasets for base data creation.
    Reviews, check-ins, and tips are processed as streams.
    
    Data Transformations
    User data is transformed by converting dates to timestamps and splitting the elite column into an array.
    Business data undergoes transformations like converting categories and attributes columns into arrays.
    
### Step 2: Sentiment Analysis (sentiment.py)
    NLTK for Natural Language Processing
    Utilize NLTK's SentimentIntensityAnalyzer for determining the sentiment of review texts.
    Tokenization and frequency analysis of words to identify key terms used in reviews.
    
### Step 3: Streaming Data Ingestion (data_ingestion.py)
    Kafka Streaming Integration
    Setup of Kafka consumers to ingest review, tip, and check-in data.
    Data batches are read from Kafka topics and written to Parquet files after processing.
### Step 4: Business Attributes (attributes/business.py)
    Extraction of Business Features
    Derive attributes like user visit frequency to various business categories and preferences.
    Geographical data extraction to identify user locations and popular destinations.
    
### Step 5: Review Attributes (attributes/review.py)
    Analyzing Review Content
    Count and analyze sentiments expressed in reviews.
    Identify and count the most frequent words in reviews for deeper insights.
    
### Step 6: User Attributes (attributes/users_agg.py)
    User Profile Aggregation
    Aggregate data to compute average ratings, visit frequencies, and other user-centric metrics.
    Calculate user inactivity periods and diversity in business visits.
    
### Step 7: Feature Aggregation (feature_aggregator.py)
    Combining All Features
    Merge attributes from user, business, and review datasets.
    Create a multi-dimensional table capturing various aspects of user behavior and preferences.
    
### Execution and Deployment
    Initialization: Setup of Spark sessions and data paths.
    Sequential Processing: Execute scripts in order for data processing and analysis.
    Final Aggregation: Compile the final dataset with aggregated features.
    Output Generation: Store data in Parquet format for efficient access.
    
    
### Technologies Used
    PySpark: For distributed data processing.
    Apache Kafka: For real-time data streaming.
    NLTK: For natural language processing.
    Parquet: For data storage.


To use tableu dashboard - please use below Snowflake credentials. "password = Project228"

Streamlit app url, its connected to the same database. - https://yelp-customer-segmentation.streamlit.app/ 


### Package Dependency
``` shell
pip install sqlalchemy
pip install ipynb
pip install snowflake-connector-python
pip install snowflake-sqlalchemy
pip install tqdm
pip install nltk
pip install wordCloud

pip install streamlit
pip install watchdog
pip install yfinance

# for this we need python 3.8+
pip install snowflake-snowpark


```


### Snowflake connection
```
account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data.project@gmail.com"

snowflake_options = {
    "sfURL": "https://fx34478.us-central1.gcp.snowflakecomputing.com",
    "sfUser": "DATA_PROJECT",
    "sfPassword": "Project",
    "sfWarehouse": "COMPUTE_WH",
    "sfDatabase": "data_project",
    "sfSchema": "yelp",
    "sfTable": "test",
    "dbtable": "test"
}
```

### StreamLit App

https://yelp-customer-segmentation.streamlit.app/
