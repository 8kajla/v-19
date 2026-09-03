FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m compileall -q . && python -m pytest -q
ENV PYTHONUNBUFFERED=1
CMD ["python","bot.py"]
