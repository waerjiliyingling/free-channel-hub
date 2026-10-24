FROM docker.1ms.run/python:3.10-slim

WORKDIR /app

COPY requirements.txt .
COPY app.py .

RUN pip install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "app.py"]
