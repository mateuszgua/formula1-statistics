# Formula 1 Statistics Site
> This application shows statistics on Formula 1 racings.
> Live demo [_here_](#).

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Screenshots](#screenshots)
* [Setup](#setup)
* [Usage](#usage)
* [Project Status](#project-status)
* [Room for Improvement](#room-for-improvement)
* [Contact](#contact)


## General Information
- This project was created becouse I wanted to learn how to use PySpark. 
- I wanted to create database in SQL Server and create ETL process.
- I wanted learn basic Power BI function.


## Technologies Used
- Python - version 3.11.2
- Pyspark - version 3.4.0

## Features
List the ready features here:
- Creating sorce in S3 bucket, 
- Creating connection with S3 bucket,
- Making connection with DB,
- Creating database and tables,

## Screenshots
![Example screenshot](./app/static/func-diagram.png)


## Setup
For start application with docker you need [Docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/).


## Usage
The application can be build from sources or can be run in docker.

##### Build from sources
```bash
$ # Move to directory
$ cd folder/to/clone-into/
$
$ # Clone the sources
$ git clone 
$
$ # Move into folder
$ cd 
$
$ # Create virtual environment
$ python3 -m venv venv
$
$ # Activate the virtual environment
$ source venv/bin/activate
$
$ # Install requirements.txt file
$ pip install -r requirements.txt
$
$ # Start app
$ flask --app app.py run
$ # ...
$ # * Running on http://127.0.0.1:5000 
```

##### Start the app in Docker
```bash
$ # Move to directory
$ cd folder/to/clone-into/
$
$ # Clone the sources
$ git clone 
$
$ # Move into folder
$ cd 
$
$ # Start app
$ docker-compose up --build
$ # ...
$ # frontend_1  |  * Running on http://127.0.0.1:5000
```

##### Copy image from DockerHub
An Image has been created for the application, the image pushed on [DockerHub](#).
```bash
$ # Get image from dockerhub
$ docker pull 
$
$ # Rename image
$ docker image tag 
$
$ # Delete old image
$ docker rmi 
$
$ # Create network
$ docker network create mynetwork
$
$ # Run mysql container
$ docker run --rm --name 
$
$ # Run app container
$ docker run --rm --name 
```

## Project Status
Project is: in_progress


## Room for Improvement
Room for improvement:
- 

## Contact
Created by [@DevGua](https://devgua-portfolio.web.app/) - feel free to contact me!