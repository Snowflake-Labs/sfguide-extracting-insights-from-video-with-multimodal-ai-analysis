-- COMMON SETUP
USE ROLE accountadmin;

CREATE ROLE hol_user_role;

CREATE DATABASE IF NOT EXISTS hol_db;
GRANT OWNERSHIP ON DATABASE hol_db TO ROLE hol_user_role COPY CURRENT GRANTS;

CREATE OR REPLACE WAREHOUSE hol_warehouse WITH
  WAREHOUSE_SIZE='X-SMALL';
GRANT USAGE ON WAREHOUSE hol_warehouse TO ROLE hol_user_role;

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE hol_user_role;

-- Create a compute pool with one Medium GPU instance that suspends after 1 hour of inactivity.
CREATE COMPUTE POOL hol_compute_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = GPU_NV_M
  AUTO_SUSPEND_SECS = 3600;

GRANT USAGE, MONITOR ON COMPUTE POOL hol_compute_pool TO ROLE hol_user_role;
GRANT OWNERSHIP ON SCHEMA hol_db.public TO ROLE hol_user_role;
GRANT CREATE NETWORK RULE ON SCHEMA hol_db.public TO ROLE accountadmin;  

CREATE OR REPLACE NETWORK RULE hol_db.public.dependencies_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hf.co', 'huggingface.co', 'cas-bridge.xethub.hf.co', 'pypi.python.org', 'pypi.org', 'cdn.pypi.org','pythonhosted.org', 'files.pythonhosted.org', 'github.com', 'githubusercontent.com');

CREATE EXTERNAL ACCESS INTEGRATION dependencies_access_integration
  ALLOWED_NETWORK_RULES = (hol_db.public.dependencies_network_rule)
  ENABLED = true;

GRANT USAGE ON INTEGRATION dependencies_access_integration TO ROLE hol_user_role;

GRANT CREATE STREAMLIT ON SCHEMA hol_db.public TO ROLE hol_user_role;
SET name = (SELECT CURRENT_USER());
GRANT ROLE hol_user_role TO USER IDENTIFIER($name);

USE ROLE hol_user_role;
USE DATABASE hol_db;
USE WAREHOUSE hol_warehouse;

CREATE IMAGE REPOSITORY IF NOT EXISTS repo;

CREATE STAGE IF NOT EXISTS videos
  DIRECTORY = ( ENABLE = true )
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
CREATE STAGE IF NOT EXISTS model
  DIRECTORY = ( ENABLE = true )
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- MEETING SOURCE SETUP
-- If using SnowSQL locally: put files onto the stage from the local videos folder. If using Snowsight: upload from UI
PUT file://videos/amicorpus/IS1004/audio/*.mp3 @videos/amicorpus/IS1004/audio AUTO_COMPRESS=TRUE;
PUT file://videos/amicorpus/IS1004/video/*.mp4 @videos/amicorpus/IS1004/video AUTO_COMPRESS=TRUE;
PUT file://videos/amicorpus/IS1004/slides/*.jpg @videos/amicorpus/IS1004/slides AUTO_COMPRESS=TRUE;
PUT file://chatbot/meeting_analysis.yaml @model AUTO_COMPRESS=TRUE;


SET meeting_id = 'IS1004';
SET meeting_part = 'IS1004c';

-- VIDEO ANALYSIS

-- Build Docker image on command line and publish to SPCS Image Registry. 

-- Replace <image>:<version> below
SHOW IMAGE REPOSITORIES IN SCHEMA hol_db.public
SHOW IMAGES IN IMAGE REPOSITORY hol_db.public.repo;

DROP SERVICE IF EXISTS process_video;
EXECUTE JOB SERVICE
  IN COMPUTE POOL hol_compute_pool
  ASYNC=TRUE
  NAME = process_video
  EXTERNAL_ACCESS_INTEGRATIONS=(dependencies_access_integration)
  FROM SPECIFICATION_TEMPLATE $$
spec:
  containers:
    - name: qwen25vl
      image: <image>:<version>
      resources:
        requests:
          nvidia.com/gpu: 4
        limits:
          nvidia.com/gpu: 4
      env:
        MEETING_ID: {{id}}
        MEETING_PART: {{part}}
        VIDEO_PATH: /videos/amicorpus/{{id}}/video/{{part}}.C.mp4
        SNOWFLAKE_WAREHOUSE: hol_warehouse
        HF_TOKEN: <your_hf_token>
        PROMPT: Provide a detailed description of this meeting video, dividing it in to sections with a one sentence description, and capture the most important text that's displayed on screen. Identify the start and end of each section with a timestamp in the 'hh:mm:ss' format. Return the results as JSON
        OUTPUT_TABLE: video_analysis
        FPS: 0.25
      volumeMounts:
        - name: videos
          mountPath: /videos
        - name: dshm
          mountPath: /dev/shm
  volumes:
    - name: dshm
      source: memory
      size: 10Gi
    - name: videos
      source: "@videos"
  platformMonitor:
    metricConfig:
      groups:
      - system
  networkPolicyConfig:
    allowInternetEgress: true
      $$
      USING(id=> $meeting_id, part=> $meeting_part);

SELECT * FROM video_analysis;

-- OCR
CREATE OR REPLACE TABLE slides_analysis (
    meeting_id STRING,
    meeting_part STRING,
    image_path STRING,
    text_content STRING
);

INSERT INTO slides_analysis
SELECT
    $meeting_id as meeting_id,
    $meeting_part as meeting_part,
    relative_path AS image_path,
    CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        @videos,
        relative_path,
        {'mode': 'LAYOUT'}
    ):content AS STRING) AS text_content
FROM DIRECTORY(@videos)
WHERE relative_path LIKE CONCAT('amicorpus/', $meeting_id, '/slides/%.jpg');

SELECT * FROM slides_analysis;


-- SPEECH RECOGNITION
CREATE OR REPLACE TABLE speech_analysis (
    meeting_id STRING,
    meeting_part STRING,
    audio_path STRING,
    text_content STRING
);

SET audio_path = CONCAT('@videos/amicorpus/', $meeting_id, '/audio/', $meeting_part, '.Mix-Lapel.mp3');

INSERT INTO speech_analysis
SELECT
    $meeting_id as meeting_id,
    $meeting_part as meeting_part,
    $audio_path AS audio_path,
    CAST(SNOWFLAKE.CORTEX.AI_TRANSCRIBE(TO_FILE($audio_path)):text AS STRING) AS text_content

SELECT * FROM speech_analysis;

-- CLEANUP
-- Drop Streamlit by title
DECLARE
  app_name STRING;
BEGIN
  SELECT 
    STREAMLIT_NAME INTO :app_name 
    FROM INFORMATION_SCHEMA.STREAMLITS 
    WHERE STREAMLIT_TITLE = 'MEETING_CHAT'
    LIMIT 1;
  EXECUTE IMMEDIATE 
    'DROP STREAMLIT hol_db.public.' || :app_name;
END;

DROP DATABASE hol_db;
DROP EXTERNAL ACCESS INTEGRATION dependencies_access_integration;

USE ROLE accountadmin;
DROP COMPUTE POOL hol_compute_pool;
DROP WAREHOUSE hol_warehouse;
DROP ROLE hol_user_role;