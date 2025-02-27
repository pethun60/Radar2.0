# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY check_trend_v2.py /app/

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
# USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
# Use $(pwd) inside CMD to dynamically set the working directory
# CMD ["python", "check_trend_v2.py", "--t", "1800", "--d", "$(pwd)", "--w", "trend_files"]
CMD ["python", "check_trend_v2.py", "--t", "1800",  "--w", "trend_files", "--url", "influxdb_cont:8086" ]