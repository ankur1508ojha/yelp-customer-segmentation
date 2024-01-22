conda create -n python310 python=3.10
conda activate python310
pip install kafka-python




# Execution
cd /Users/ankur/Documents/drive/jobs_prep/yelp-customer-segmentation

#### Kafka Setup and testing #####

    # setup kafka docker image
    cd /Users/ankur/Documents/drive/jobs_prep/yelp-customer-segmentation/setup/kafka/bitnami

    # start kafka docker container
    docker-compose up -d

    # testing the kafka
    cd /Users/ankur/Documents/drive/jobs_prep/yelp-customer-segmentation/scripts
    pwd # should be yelp-customer-segmentation

    python3 kafka/producer.py localhost:9094 reviews /Users/ankur/data_engineering/yelp_dataset/yelp_academic_dataset_review.json 1000

### Data Prepration

pip install pyspark
pip install nltk

###### Running the main pipeline and this can be put as Airflow Dag
cd scripts

# One time data prep
python data_processing.py /Users/ankur/data_engineering/yelp_dataset/ /Users/ankur/data_engineering/yelp_dataset/output/ 0.0001

# Stream ingestiong and data update - this will read review data from kafka and enrich and write it
 python data_ingestion.py localhost:9094 reviews /Users/ankur/data_engineering/yelp_dataset/output/ 0.0001


 # final feature aggregation
 python3 feature_aggregator.py 0.0001
