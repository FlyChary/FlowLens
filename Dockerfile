FROM python:3.12-slim
WORKDIR /app
ENV PYTHONHASHSEED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY pyproject.toml ./
COPY flowlens ./flowlens
RUN pip install --no-cache-dir .
EXPOSE 8080
CMD ["python", "-m", "flowlens", "--port", "8080"]
