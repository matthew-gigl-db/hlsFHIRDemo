# The 'data_sources' folder contains definitions for all data sources
# used by the pipeline.
# full array patients, encounters, claims_transactions, conditions, medications, claims, devices, payers, imaging_studies

# Keeping 'data_sources' separate provides a clear overview of the data used
# and allows for easy swapping of sources during development.

from utilities.bronze import Bronze

file_types = spark.conf.get("file_types").split(',')
file_types = [file_type.strip() for file_type in file_types]

for file_type in file_types:
    BronzePipeline = Bronze(
        spark = spark
        ,catalog = spark.conf.get("catalog_use")
        ,schema = spark.conf.get("schema_use")
        ,volume = spark.conf.get("volume_use")
        ,volume_sub_path = spark.conf.get("volume_sub_path_use")
        ,file_type = file_type
        ,cleanSource_moveDestination = spark.conf.get("cleanSource_moveDestination")
        ,cleanSource_deleteDestination = spark.conf.get("cleanSource_deleteDestination")
    )
    
    BronzePipeline.stream_ingest()