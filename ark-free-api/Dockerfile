FROM docker.1ms.run/python:3.10-slim

WORKDIR /app

COPY requirements.txt .
COPY app.py .

RUN sed -i "s|deb.debian.org|mirrors.tuna.tsinghua.edu.cn|g" /etc/apt/sources.list.d/debian.sources 2>/dev/null; \
    apt-get update && \
    pip install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple --no-cache-dir -r requirements.txt && \
    playwright install chromium --with-deps

EXPOSE 8000

CMD ["python", "app.py"]
