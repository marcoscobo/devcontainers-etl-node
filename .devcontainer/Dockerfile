FROM python:3.13

# Copy requirements.txt and then install Python dependencies
COPY ../requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /workspace

# Keep the container running
CMD ["sleep", "infinity"]