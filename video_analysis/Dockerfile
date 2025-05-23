FROM vllm/vllm-openai:v0.7.2
COPY ./ /app
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]