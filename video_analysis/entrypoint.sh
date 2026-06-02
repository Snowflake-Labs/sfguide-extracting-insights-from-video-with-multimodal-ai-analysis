#!/bin/bash

# Install dependencies
python3 -m pip install --upgrade pip
pip install git+https://github.com/huggingface/transformers@f3f6c86582611976e72be054675e2bf0abb5f775
pip install accelerate
# Some code in this repo relies on a specific version of qwen
# Without this change there will be an error "TypeError: Qwen2_5_VLProcessingInfo.get_hf_processor() got an unexpected keyword argument 'do_sample_frames'"
pip install qwen-vl-utils==0.0.10
pip install click
pip install snowflake-connector-python

# Add optional args
if [ -n "$FPS" ]; then
  optional_args+=("--fps" "$FPS")
fi

# Running job code
python3 -u /app/run.py --video-path $VIDEO_PATH --prompt "$PROMPT" --output-table $OUTPUT_TABLE --meeting-id $MEETING_ID --meeting-part $MEETING_PART "${optional_args[@]}"
