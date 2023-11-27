from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
from snowflake.snowpark.types import VariantType
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import udf

import cachetools
import easyocr
import json

def create_session():
    """
    Function to create session for the UDF build. 
    The connection data is stored in the file connection.json residing in the same folder.
    """
    session = Session.builder.configs(json.load(open("connection.json"))).create()
    return session

@cachetools.cached(cache={})
def get_import_dir():
    """
    Function to read the import directory name for the current user
    """
    import sys
    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
    return import_dir

@cachetools.cached(cache={})
def prepare_model(model_dir):
    """
    Function to copy and merge the model files for EasyOCR
    """
    import shutil
#
    model_dir += "/"
    import_dir = get_import_dir()
#
    craft_model_part1 = import_dir +  "craft_mlt_25k.pth.1"
    craft_model_part2 = import_dir +  "craft_mlt_25k.pth.2"
    craft_model_part3 = import_dir +  "craft_mlt_25k.pth.3"
    craft_model = model_dir + "craft_mlt_25k.pth"
#
    with open(craft_model,'wb') as wfd:
        for f in [craft_model_part1,craft_model_part2,craft_model_part3]:
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd)
#
    english_language_model = "english_g2.pth"
    shutil.copyfile(import_dir + english_language_model, model_dir + english_language_model)

def load_image(image_bytes_in_str):
    """
    Function to load the image file from Snowflake into a temporary folder
    """
    import os
    image_file = '/tmp/' + str(os.getpid())
    image_bytes_in_hex = bytes.fromhex(image_bytes_in_str)
    #
    with open(image_file, 'wb') as f:
        f.write(image_bytes_in_hex)
    return image_file

@cachetools.cached(cache={})
def initialize_reader(model_dir):
    """
    Function to initalize the OCR model outside of the UDF
    """
    return easyocr.Reader(lang_list = ['en'], model_storage_directory = model_dir, user_network_directory = model_dir)
    
session = Session.builder.configs(json.load(open("connection.json"))).create()

@udf(name='extract_data_from_image',session=session,replace=True,is_permanent=True,stage_location='@UPLOAD_STAGE',
input_types=[StringType(16777216)], return_type=VariantType(),
packages = ['cachetools==4.2.2','easyocr==1.7.0','joblib==1.2.0','pillow==9.4.0','snowflake-snowpark-python','torchvision==0.15.2'],
imports = ['@IMAGE_UPLOAD.PUBLIC.UPLOAD_STAGE/english_g2.pth','@IMAGE_UPLOAD.PUBLIC.UPLOAD_STAGE/craft_mlt_25k.pth.3','@IMAGE_UPLOAD.PUBLIC.UPLOAD_STAGE/craft_mlt_25k.pth.2','@IMAGE_UPLOAD.PUBLIC.UPLOAD_STAGE/craft_mlt_25k.pth.1']
)
def extract_data_from_image(img):
    """
    Function to return the extracted metadata from the image
    This is the actual UDF in Snowflake
    """
    import re
    model_dir = "/tmp"
    prepare_model(model_dir)
    reader = initialize_reader(model_dir)
#    
    img_text = reader.readtext(load_image(img))
    decadeclassification_string = img_text[0][1]
    nnd_string = img_text[1][1]
#
    declassification_pattern = r'Declassified per Executive Order\s+(.*)\s*, Section\s+(.*)'
    nnd_pattern = r'NND Project Number:\s*(.*?)\s*By:\s*(.*)NND Date:\s*(.*)'
#
    if match := re.search(declassification_pattern, decadeclassification_string, re.IGNORECASE):
      order, section = match.groups()
#
    if match := re.search(nnd_pattern, nnd_string, re.IGNORECASE):
      project_number, author, project_date = match.groups()
#
    return [order, section, project_number, author, project_date]

session.close()