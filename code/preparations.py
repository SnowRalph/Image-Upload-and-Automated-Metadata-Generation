# Run: pip install -r requirements.txt
from snowflake.snowpark.session import Session
import json

print("Preparing database and stage")
# Create session to database. Please configure your connection file connection.json
session = Session.builder.configs(json.load(open("connection.json"))).create()

# Create database and stage
session.sql("CREATE OR REPLACE DATABASE IMAGE_UPLOAD;").collect()
session.sql("USE DATABASE IMAGE_UPLOAD;").collect()
session.sql("""CREATE STAGE UPLOAD_STAGE
               ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
               DIRECTORY = ( ENABLE = true );""").collect()


# This table stores the image files uploaded from the Streamlit App
session.sql("""CREATE OR REPLACE TABLE IMAGES 
                   (IMAGE_ID STRING, 
                    IMAGE_BYTES STRING
                   );""").collect()

# This table stores the metadata for the image files uploaded from the Streamlit App
session.sql("""CREATE OR REPLACE TABLE IMAGE_METADATA 
                   (IMAGE_ID STRING, 
                    FILE_NAME STRING, 
                    EXECUTIVE_ORDER STRING, 
                    SECTION STRING, 
                    PROJECT_NUMBER STRING, 
                    AUTHOR STRING, 
                    PROJECT_YEAR STRING
                   );""").collect()


# Copy the CRAFT model files to the stage. 
# The original model file craft_mlt_25k.pth is to big for the upload.
# This part-files will be autoamtically concatenated in a later stage.
print("Copying CRAFT model files to the stage")
session.file.put("../import/craft_mlt_25k.pth.*", "@UPLOAD_STAGE", auto_compress = False)

# Copy the English language model file to the stage
print("Copying English language model file to the stage")
session.file.put("../import/english_g2.pth", "@UPLOAD_STAGE", auto_compress = False)

session.close()