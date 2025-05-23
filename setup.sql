-- common setup
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
GRANT USAGE, MONITOR ON COMPUTE POOL hol_compute_pool TO ROLE container_user_role;

GRANT ROLE container_user_role TO USER <user_name>

USE ROLE container_user_role;
USE DATABASE hol_db;
USE WAREHOUSE hol_warehouse;

CREATE IMAGE REPOSITORY IF NOT EXISTS repo;
CREATE STAGE IF NOT EXISTS meetings
  DIRECTORY = ( ENABLE = true );

-- TODO SET UP EAI

DROP SERVICE process_video FORCE;
EXECUTE JOB SERVICE
  IN COMPUTE POOL hol_compute_pool
  NAME = process_video
  EXTERNAL_ACCESS_INTEGRATIONS=(allow_all_aei)
  FROM SPECIFICATION $$
spec:
  containers:
    - name: qwen25vl
      image: pm-pm-aws-us-west-2.registry.snowflakecomputing.com/yavorg_container_hol_db/public/image_repo/qwen2.5vl:v0.42
      resources:
        requests:
          nvidia.com/gpu: 4
        limits:
          nvidia.com/gpu: 4
      env:
        HF_TOKEN: <your_hf_token>
        VIDEO_PATH: /videos/amicorpus/IS1004/video/IS1004c.C.mp4
        PROMPT: Provide a detailed description of this meeting video, dividing it in to sections with a one sentence description, and capture the most important text that's displayed on screen. Identify the start and end of each section with a timestamp in the 'mm:ss' format. Return the results as JSON
        OUTPUT_TABLE: video_analysis
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
      source: "@meetings"
  platformMonitor:
    metricConfig:
      groups:
      - system
  networkPolicyConfig:
    allowInternetEgress: true
      $$;
