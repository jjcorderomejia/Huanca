FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.31.0

COPY generator.py .

RUN useradd -m -u 1000 generator
USER generator

CMD ["python", "-u", "generator.py"]
