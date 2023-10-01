# Use an official Python runtime as a parent image
FROM python:3.8
# Set the working directory to /pizza-service
WORKDIR /pizza-service

# Copy the current directory contents into the container at /pizza-service
COPY . /pizza-service
# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt
# Make port 80 available to the world outside this container
# Run app.py when the container launches

ENV KAFKA_BROKER="localhost:9092"

CMD ["python", "app.py"]
