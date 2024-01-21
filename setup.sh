conda create -n python310 python=3.10
conda activate python310
pip install kafka-python




# Execution
cd yelp-customer-segmentation/

#### Kafka Setup and testing #####

    # setup kafka docker image
    cd setup/kafka/bitnami

    # start kafka docker container
    docker-compose up -d

    # testing the kafka
    cd ../../../
    pwd # should be yelp-customer-segmentation

    python3 scripts/kafka/producer.py localhost:9094 reviews ~/Downloads/yelp_review.json 0

### Data Prepration

pip install pyspark

###### Running the main pipeline and this can be put as Airflow Dag
cd scripts

# One time data prep
python data_processing.py /Users/hims/Downloads/yelp_dataset/ /Users/hims/Downloads/yelp_dataset/output/ 0.0001

# Stream ingestiong and data update - this will read review data from kafka and enrich and write it
 python data_ingestion.py localhost:9094 reviews /Users/hims/Downloads/yelp_dataset/output/ 0.0001


 # final feature aggregation
 python3 feature_aggregator.py 0.1
