# build docker image
docker build --tag luigi:v1 .

# add location on your machine where you would like to save luigi files created within a container and run a container from built image
docker run --volume "<LOCAL_DIR>:/home/dfltuser/data" --name luigicontainer luigi:v1