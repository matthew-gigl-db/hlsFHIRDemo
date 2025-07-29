import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit, concat
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType

class Bronze:
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, volume_sub_path: str, file_type: str, redox_extract_volume: str, cleanSource_retentionDuration: str, cleanSource: str = "OFF"):
        self._spark = spark
        self._catalog = catalog
        self._schema = schema
        self._volume = volume
        self._volume_sub_path = volume_sub_path
        self._file_type = file_type
        self._redox_extract_volume = redox_extract_volume
        self._cleanSource_retentionDuration = cleanSource_retentionDuration
        self._cleanSource = cleanSource

    """
    The Bronze class represents a data structure for managing metadata related to a specific data resource.
    
    Attributes:
        spark (SparkSession): The SparkSession object used for interacting with the Spark runtime.
        catalog (str): The catalog name where the data is stored.
        schema (str): The schema name within the catalog.
        volume (str): The volume name within the schema.
        volume_sub_path (str): The sub-path within the volume where the data is located.
        file_type (str): The type of the clinical data resource, e.g. "ccda", "fhir", etc.
        cleanSource_retentionDuration (str): The retention duration for the cleanSource.
        cleanSource (str): The cleanSource for the data.
        redox_extract_volume (str): The volume name within the schema where the original files are copied while processing into bronze and extracted by Redox for conversion.  

    Methods:
        __repr__(): Returns a string representation of the Bronze object.
        stream_ingest(): Defines a Delta Live Table for streaming ingestion of files using Auto Loader, applies file copy logic, and writes metadata and ingestion status.
        to_dict(): Converts the Bronze object attributes to a dictionary for serialization or logging.
        from_dict(cls, data): Creates a Bronze object from a dictionary of attributes.
    """

    def __repr__(self):
        return (
            f"Bronze("
            f"spark={self._spark!r}, "
            f"catalog='{self._catalog}', "
            f"schema='{self._schema}', "
            f"volume='{self._volume}', "
            f"volume_sub_path='{self._volume_sub_path}', "
            f"file_type='{self._file_type}', "
            f"redox_extract_volume='{self._redox_extract_volume}', "
            f"cleanSource_retentionDuration='{self._cleanSource_retentionDuration}', "
            f"cleanSource='{self._cleanSource}'"
            f")"
        )

    @property
    def catalog(self):
        return self._catalog

    @staticmethod
    @udf(MapType(StringType(), StringType()))
    def copy_file(src_path, dest_path):
      try:
        with open(src_path, 'rb') as src_file, open(dest_path, 'wb') as dest_file:
          dest_file.write(src_file.read())
        return {"status": "success"}
      except Exception as e:
        return {"status": "error", "message": str(e)}
      
    def stream_ingest(self):
      schema_definition = f"""
        file_metadata STRUCT < file_path: STRING, 
        file_name: STRING,
        file_size: BIGINT,
        file_block_start: BIGINT,
        file_block_length: BIGINT,
        file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.'
        ,ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.'
        ,value STRING COMMENT 'The raw {self._file_type} file contents.'
        ,sent_to_redox MAP<STRING, STRING> COMMENT 'Status of the file being sent to Redox, with an error message on failure.'
      """

      if self._volume_sub_path == None:
        volume_path = f"/Volumes/{self._catalog}/{self._schema}/{self._volume}/{self._file_type}"
      else:
        volume_path = f"/Volumes/{self._catalog}/{self._schema}/{self._volume}/{self._volume_sub_path}/{self._file_type}"

      extract_path = f"/Volumes/{self._catalog}/{self._schema}/{self._redox_extract_volume}/{self._file_type}/"

      @dlt.table(
        name=f"{self._catalog}.{self._schema}.{self._file_type}_bronze",
        comment=f"Streaming bronze ingestion of {self._file_type} files from {volume_path}",
        # spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
        table_properties={
          'quality' : 'bronze'
          ,'delta.enableChangeDataFeed' : 'true'
          ,'delta.enableDeletionVectors' : 'true'
          ,'delta.enableRowTracking' : 'true'
        },
        # path="<storage-location-path>",
        # partition_cols=["<partition-column>", "<partition-column>"],
        cluster_by = ["file_metadata.file_path"],
        schema=schema_definition,
        # row_filter = "row-filter-clause",
        temporary=False
      )
      # @dlt.expect(...)
      def stream_ingest_function():
          return (
            self._spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholeText", "true")
            .option("cloudFiles.cleanSource", self._cleanSource)
            .option("cloudFiles.cleanSource.retentionDuration", self._cleanSource_retentionDuration)
            .load(volume_path)
            .selectExpr("_metadata as file_metadata", "*")
            .withColumn("source_path", col("file_metadata.file_path"))
            .withColumn("extraction_path",  concat(lit(extract_path), col("file_metadata.file_name")))
            .withColumn("sent_to_redox", self.copy_file(col("source_path"), col("extraction_path")))
            .drop("source_path", "extraction_path")
            # .withColumn(
            #   "parsed_value",
            #   from_xml(
            #     col("value"),
            #     schema_of_xml(col("value")),
            #     {
            #       "rowTag": "ClinicalDocument",
            #       "rootTag": "root",
            #       "nullValue": "",
            #       "mode": "PERMISSIVE"
            #     }
            #   )
            # )
          )

    def to_dict(self):
        return {
            "spark": self._spark,
            "catalog": self._catalog,
            "schema": self._schema,
            "volume": self._volume,
            "volume_sub_path": self._volume_sub_path,
            "file_type": self._file_type,
            "redox_extract_volume": self._redox_extract_volume,
            "cleanSource_retentionDuration": self._cleanSource_retentionDuration,
            "cleanSource": self._cleanSource
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data['spark'],
            data['catalog'],
            data['schema'],
            data['volume'],
            data['volume_sub_path'],
            data['file_type'],
            data['redox_extract_volume'],
            data['cleanSource_retentionDuration'],
            data.get('cleanSource', "OFF")
        )









