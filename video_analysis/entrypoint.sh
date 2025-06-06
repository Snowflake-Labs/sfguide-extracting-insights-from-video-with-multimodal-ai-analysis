#!/bin/bash

# Install dependencies
python3 -m pip install --upgrade pip
pip install git+https://github.com/huggingface/transformers@f3f6c86582611976e72be054675e2bf0abb5f775
pip install accelerate
pip install qwen-vl-utils
pip install click
pip install snowflake-connector-python

# Add optional args
if [ -n "$FPS" ]; then
  optional_args+=("--fps" "$FPS")
fi

# Running job code
python3 -u /app/run.py --video-path $VIDEO_PATH --prompt "$PROMPT" --output-table $OUTPUT_TABLE --meeting-id $MEETING_ID --meeting-part $MEETING_PART "${optional_args[@]}"