FROM docker.1ms.run/python:3.10-slim
WORKDIR /app
# 构建上下文是上一级 free-channel-hub/，文件在那里
COPY mock_tool_api.py .
EXPOSE 8000
CMD ["python", "mock_tool_api.py", "8000"]
