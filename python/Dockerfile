# One image, two processes (same code, different command):
#   API       docker run ... fleet-telemetry-python                          (default CMD, port 8080)
#   consumer  docker run ... fleet-telemetry-python python -m app.consumer
#
# Build from the python/ folder:  docker build -t fleet-telemetry-python python/
# See the README ("Run it in Docker") for the network / environment flags.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv

# Dependencies first: this layer is rebuilt only when requirements.txt changes, not on every code edit.
# (requirements.txt also lists the test tools; they are small, and one file keeps local dev and the image in sync.)
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

# Do not run as root.
RUN useradd --system --uid 10001 --no-create-home app
USER app

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
