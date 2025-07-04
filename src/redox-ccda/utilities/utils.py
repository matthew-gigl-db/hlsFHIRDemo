from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType

def copy_file(src_path, dest_path):
    try:
        with open(src_path, 'rb') as src_file, open(dest_path, 'wb') as dest_file:
            dest_file.write(src_file.read())
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

copy_file_udf = udf(copy_file, MapType(StringType(), StringType()))