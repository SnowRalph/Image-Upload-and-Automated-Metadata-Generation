# Snowpark for Python Developer Guide: https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html
# Streamlit docs: https://docs.streamlit.io/
# EasyOCR: https://github.com/JaidedAI/EasyOCR/tree/master

import json
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st
import io
from io import StringIO
import base64
import uuid
from datetime import datetime, timedelta

# Streamlit config
st.set_page_config(
    page_title="Image Upload and Metadata Generation App in Snowflake",
    layout='wide',
    menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "The source code for this application can be accessed on GitHub https://github.com/Snowflake-Labs/sfguide-snowpark-pytorch-streamlit-openai-image-rec"
     }
)

# Set page title, header and links to docs
st.header("Image Upload and Automated Metadata Generation App in Snowflake using Snowpark Python, EasyOCR and Streamlit")
st.caption(f"""App developed by Ralph 
(Credits to [Dash](https://twitter.com/iamontheinet) for the great introduction into the topic in [A Image Recognition App in Snowflake ...](https://quickstarts.snowflake.com/guide/image_recognition_snowpark_pytorch_streamlit_openai/index.html#0))""")
st.write("[Resources: [Snowpark for Python Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)   |   [Streamlit](https://docs.streamlit.io/)   |   [EasyOCR](https://github.com/JaidedAI/EasyOCR)]")

def create_session():
    """
    Function to create new or get existing Snowpark session
    """
    if "snowpark_session" not in st.session_state:
        session = Session.builder.configs(json.load(open("connection.json"))).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']
    return session

def upsize():
    """
    Upsize the data warehouse for EasyOCR 
    """
    session.sql("""CREATE OR REPLACE WAREHOUSE compute_wh WITH
                       WAREHOUSE_SIZE = 'MEDIUM'
                       WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED';""").collect()

def downsize():
    """
    Downsize the data warehouse 
    """
    session.sql("""CREATE OR REPLACE WAREHOUSE compute_wh WITH
                       WAREHOUSE_SIZE = 'X-SMALL';""").collect()

def suspend():
    """
    Suspend the data warehouse for downsizing 
    """
    session.sql("""ALTER WAREHOUSE compute_wh SUSPEND;""").collect()

# Call function to create new or get existing Snowpark session to connect to Snowflake
session = create_session()

uploaded_file = st.file_uploader("Choose an image file", accept_multiple_files=False, label_visibility='hidden')
if uploaded_file is not None:

    with st.spinner("Uploading image, generating metadata and inserting in real-time..."):
        start_time = datetime.now()
        # Convert image base64 string into hex 
        bytes_data_in_hex = uploaded_file.getvalue().hex()

        # Generate new image file name
        image_id = 'img_' + str(uuid.uuid4())

        # Write image data in Snowflake table
        df = pd.DataFrame({"IMAGE_ID": [image_id], "IMAGE_BYTES": [bytes_data_in_hex]})
        session.write_pandas(df, "IMAGES")

        upsize()
        # Insert image metadata into table
        res = session.sql(f"""INSERT INTO IMAGE_METADATA
                              WITH METADATA 
                              AS (SELECT EXTRACT_DATA_FROM_IMAGE(IMAGE_BYTES) M 
                                  FROM IMAGES WHERE IMAGE_ID = '{image_id}')
                                  SELECT '{image_id}' IMAGE_ID,
                                         '{uploaded_file.name}' FILE_NAME,
                                         M[0] EXECUTIVE_ORDER, 
                                         M[1] SECTION, 
                                         M[2] PROJECT_NUMBER, 
                                         M[3] AUTHOR, 
                                         M[4] PROJECT_YEAR 
                                  FROM METADATA;""").collect()

        # Get metadata information for the app 
        metadata = session.sql(f"""SELECT EXECUTIVE_ORDER, 
                                     SECTION, 
                                     PROJECT_NUMBER, 
                                     PROJECT_YEAR 
                              FROM IMAGE_METADATA
                              WHERE IMAGE_ID = '{image_id}';""").to_pandas()

        col2, col3, col4, col5, col6 = st.columns(5, gap='medium')
        with st.container():
            with col2:
                # Display uploaded image
                st.caption("Uploaded Document")
                st.image(uploaded_file)

            with col3:
                # Display column 
                st.caption("Executive Order")
                st.markdown(metadata.iloc[0,0].replace('"',''))

            with col4:
                # Display column
                st.caption("Section")
                st.markdown(metadata.iloc[0,1].replace('"',''))

            with col5:
                # Display column
                st.caption("Project Number")
                st.markdown(metadata.iloc[0,2].replace('"',''))

            with col6:
                # Display column
                st.caption("Project Year")
                st.markdown(metadata.iloc[0,3].replace('"',''))

        suspend()
        downsize()
        
        end_time = datetime.now()
        diff = end_time - start_time
        with st.container():
            st.markdown(f"Duration: {diff}")
