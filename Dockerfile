FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["fastapi", "run", "app/app.py", "--port", "80"]
