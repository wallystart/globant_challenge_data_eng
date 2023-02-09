# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:3.0-python3.8-appservice
FROM mcr.microsoft.com/azure-functions/python:3.0-python3.8
LABEL maintainer="JULIAN_BILTES <julianbiltes@gmail.com>"

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

EXPOSE 8080
EXPOSE 7071
EXPOSE 80

RUN apt-get install -y libpq-dev gcc jq

COPY ./func/requirements.txt /
RUN pip install -r /requirements.txt

COPY . /home/site/wwwroot