import click
from transformers import AutoProcessor
from vllm import LLM, SamplingParams
from qwen_vl_utils import process_vision_info
import logging
import json
import re
import os
import snowflake.connector
from datetime import datetime
# Set up logger
logger = logging.getLogger("qwen2.5vl")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s;%(levelname)s:  %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(ch)

MODEL_PATH = "Qwen/Qwen2.5-VL-7B-Instruct"

def get_login_token():
  with open("/snowflake/session/token", "r") as f:
    return f.read()
  
def get_time(time_str: str) -> datetime.time:
    ## Remove .ff at end of string
     if '.' in time_str: time_str = time_str.split('.')[0]
    
    ## Add hh: prefix if it isn't there
     if time_str.count(':') == 1: time_str = "00:" + time_str

     return datetime.strptime(time_str, "%H:%M:%S").time()


@click.command()
@click.option("--video-path", help="URL of the video to analyze")
@click.option("--fps", help="FPS to process the video", default=None, type=float)
@click.option("--prompt", help="Prompt to use for video analysis")
@click.option("--output-table", help="Output table")
@click.option("--meeting-id", help="Meeting ID for the video")
@click.option("--meeting-part", help="Meeting part for the video")
def main(video_path: str, prompt: str, output_table: str, fps: float, meeting_id: str, meeting_part: str):
    llm = LLM(
        model=MODEL_PATH,
        limit_mm_per_prompt={"image": 1, "video": 1},
        tensor_parallel_size=4,
        dtype="bfloat16",
        gpu_memory_utilization=0.9,
        max_model_len=127000
    )

    sampling_params = SamplingParams(
        temperature=0.1,
        top_p=0.001,
        repetition_penalty=1.05,
        max_tokens=2048,
        stop_token_ids=[],
    )

    video_params = {
        "type": "video", 
        "video": video_path,
        "total_pixels": 20480 * 28 * 28, 
        "min_pixels": 16 * 28 * 28
    }

    if fps is not None:
        video_params["fps"] = fps

    video_messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": [
                {"type": "text", "text": prompt},
                video_params
            ]
        }
    ]

    processor = AutoProcessor.from_pretrained(MODEL_PATH)
    prompt = processor.apply_chat_template(
        video_messages,
        tokenize=False,
        add_generation_prompt=True,
    )
    image_inputs, video_inputs, video_kwargs = process_vision_info(video_messages, return_video_kwargs=True)
    
    mm_data = {}
    if image_inputs is not None:
        mm_data["image"] = image_inputs
    if video_inputs is not None:
        mm_data["video"] = video_inputs

    llm_inputs = {
        "prompt": prompt,
        "multi_modal_data": mm_data,

        # FPS will be returned in video_kwargs
        "mm_processor_kwargs": video_kwargs,
    }

    outputs = llm.generate([llm_inputs], sampling_params=sampling_params)
    generated_text = outputs[0].outputs[0].text

    print(generated_text)

    try:
        # Parse the JSON data
        json_array_pattern = r'(\[\s*\{.*?"start_time".*?"end_time".*?"description".*?\}\s*\])'
        match = re.search(json_array_pattern, generated_text, re.DOTALL)
        
        if match:
            json_str = match.group(0)
            segments = json.loads(json_str)
        
            # Connect to Snowflake
            conn = snowflake.connector.connect(
                account= os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                host=os.getenv("SNOWFLAKE_HOST"),
                authenticator="oauth",
                token=get_login_token());
            cursor = conn.cursor()
              
            # Create the table
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {output_table} (
                meeting_id VARCHAR,
                meeting_part VARCHAR,
                video_path VARCHAR,
                start_time TIME,
                end_time TIME,
                description VARCHAR(16777216)
            )
            """)

            # Clear existing data for this meeting
            cursor.execute(f"DELETE FROM {output_table} WHERE video_path = %s", (video_path,))
            
            # Insert data
            for segment in segments:
                cursor.execute(f"""
                INSERT INTO {output_table} (meeting_id, meeting_part, video_path, start_time, end_time, description)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    meeting_id,
                    meeting_part,
                    video_path,
                    get_time(segment.get('start_time')),
                    get_time(segment.get('end_time')),
                    segment.get('description')
                ))
            
            conn.commit()
            logger.info(f"Successfully inserted {len(segments)} events into table {output_table} for video {video_path}")
        else:
            logger.error("No valid JSON array found in the generated text.")
            raise ValueError("No valid JSON array found in the generated text.")

    except Exception as e:
        logger.error(f"Error processing results: {str(e)}")
        raise

    finally:
        if 'conn' in locals() and conn:
            conn.close()


if __name__ == "__main__":
    main()
