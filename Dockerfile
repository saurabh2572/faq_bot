FROM python:3.11-slim
# RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
# RUN apt-get -y install curl
# RUN apt-get install libgomp1
COPY ./requirements.txt /app/requirements.txt
# RUN pip install --upgrade pip setuptools wheel
# install the packages from the requirements.txt file in the container
RUN pip install -r /app/requirements.txt
# copy the local app/ folder to the /app fodler in the container
COPY ./ /app
# set the working directory in the container to be the /app
WORKDIR /app
# CMD ["chainlit", "run", "app.py", "--host", "0.0.0.0", "--port", "80"]
EXPOSE 8000
ENTRYPOINT ["chainlit","run","--host", "0.0.0.0", "--port", "8000"]
CMD ["app.py"]