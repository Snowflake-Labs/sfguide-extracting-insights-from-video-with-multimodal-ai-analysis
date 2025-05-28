# sfguide-extracting-insights-from-video-with-multimodal-ai-analysis

In this guide, we’ll take text-rich videos (instructional content, meetings) and extract still images and audio. In order to perform OCR and speech recognitino using Whisper, we’ll process the images through [Snowflake Cortex AI](https://www.snowflake.com/en/product/features/cortex/) using `PARSE_DOCUMENT` and `AI_TRANSCRIBE`. To extract key moments and semantic events we will then process through Qwen2.5-VL on [Snowpark Container Services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview) (SPCS). Lastly, we will store the analysis from all three models into tables, and allow analytical queries around meeting productivity to be run on the data.

## Dataset

This repository uses the AMI Meeting Corpus dataset:

* **Source**: Edinburgh University (http://groups.inf.ed.ac.uk/ami/corpus/)
* **Citation**: Carletta, J. et al. (2005). The AMI meeting corpus: A pre-announcement. In Proc. MLMI, pp. 28-39.
* **License**: Creative Commons Attribution 4.0
* **Date Accessed**: May 22, 2025