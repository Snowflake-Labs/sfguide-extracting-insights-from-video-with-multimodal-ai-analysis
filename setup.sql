-- Common setup
USE ROLE ACCOUNTADMIN;

CREATE ROLE container_user_role;

CREATE DATABASE IF NOT EXISTS hol_db;
GRANT OWNERSHIP ON DATABASE hol_db TO ROLE container_user_role COPY CURRENT GRANTS;

CREATE OR REPLACE WAREHOUSE hol_warehouse WITH
  WAREHOUSE_SIZE='X-SMALL';
GRANT USAGE ON WAREHOUSE hol_warehouse TO ROLE container_user_role;

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE container_user_role;

CREATE COMPUTE POOL hol_compute_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = GPU_NV_M;
-- TODO add auto-suspend to cp
GRANT USAGE, MONITOR ON COMPUTE POOL hol_compute_pool TO ROLE container_user_role;

GRANT ROLE container_user_role TO USER <user_name>

USE ROLE container_user_role;
USE DATABASE hol_db;
USE WAREHOUSE hol_warehouse;

CREATE IMAGE REPOSITORY IF NOT EXISTS repo;
CREATE STAGE IF NOT EXISTS videos
  DIRECTORY = ( ENABLE = true )
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- TODO SET UP EAI

-- Meeting selection
SET meeting_id = 'IS1004';
SET meeting_part = 'IS1004c';
-- TODO expand to all 4 segments

-- Video analysis
DROP SERVICE IF EXISTS process_video;

EXECUTE JOB SERVICE
  IN COMPUTE POOL hol_compute_pool
  ASYNC=TRUE
  NAME = process_video
  EXTERNAL_ACCESS_INTEGRATIONS=(ALLOW_ALL_EAI)
  FROM SPECIFICATION_TEMPLATE $$
spec:
  containers:
    - name: qwen25vl
      image: pm-pm-aws-us-west-2.registry.snowflakecomputing.com/yavorg_container_hol_db/public/image_repo/qwen2.5vl:v0.47
      resources:
        requests:
          nvidia.com/gpu: 4
        limits:
          nvidia.com/gpu: 4
      env:
        MEETING_ID: IS1004
        MEETING_PART: IS1004c
        VIDEO_PATH: /videos/amicorpus/{{id}}/video/{{part}}.C.mp4
        SNOWFLAKE_WAREHOUSE: yavorg
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

-- OCR
CREATE OR REPLACE TABLE slides_analysis (
    image_path STRING,
    text_content STRING
);

INSERT INTO slides_analysis
SELECT
    $meeting_id,
    $meeting_part,
    relative_path AS image_path,
    CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        @videos,
        relative_path,
        {'mode': 'LAYOUT'}
    ):content AS STRING)
    AS text_content
FROM DIRECTORY(@videos)
WHERE relative_path LIKE CONCAT('amicorpus/', $meeting_id, '/slides/%.jpg')

SELECT * FROM slides_analysis;


-- asr
SELECT SNOWFLAKE.CORTEX.AI_TRANSCRIBE(
  TO_FILE(CONCAT('@videos/amicorpus/',$meeting_id,'/audio/IS1004c.Mix-Lapel.mp3')));

-- TODO cleanup
