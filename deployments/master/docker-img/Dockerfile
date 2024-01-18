# Dockerfile
FROM python:3.9-slim

# Set the working directory in the build stage
WORKDIR /docker-img

# Copy the requirements file into the build stage
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt 

# Copy the rest of your application's code
COPY . .

# Run the script
CMD ["python", "master.py"]
