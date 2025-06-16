FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

COPY starter.py  /app/starter.py
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
WORKDIR /app
CMD ["python", "starter.py", "--year", "2023", "--month", "05"]