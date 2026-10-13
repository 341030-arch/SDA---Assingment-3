# Base image: Python 3.14 slim. If your Docker registry mirror doesn't yet
# have a 3.14 slim tag available, swap this for "python:3.12-slim" -- the
# code has no 3.14-only syntax, so it will run unchanged.
FROM python:3.14-slim

WORKDIR /app

# Install dependencies first so Docker can cache this layer between builds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer source.
COPY consumer_script.py .

CMD ["python", "consumer_script.py"]
