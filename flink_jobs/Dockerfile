FROM flink:1.18.1

# --- Install Python ---
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Make python=python3
RUN ln -sf /usr/bin/python3 /usr/bin/python

# --- Install Python dependencies ---
WORKDIR /opt/flink
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Copy your PyFlink jobs ---
COPY usr_jobs/ /opt/flink/usr_jobs/
COPY download_libs.sh .
RUN chmod +x ./download_libs.sh && ./download_libs.sh

# Default job to run
CMD ["python", "/opt/flink/usr_jobs/sensor_aggregation.py"]
