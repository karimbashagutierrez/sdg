#!/bin/bash

echo "Starting cluster..."

# the default node number is 3
N=${1:-3}

docker network create --driver=bridge hadoop &> /dev/null

# start hadoop slave container
i=1
while [ $i -lt $N ]
do
	echo "start hadoop-slave$i container..."
	container_name="hadoop-slave$i"
	if docker inspect "$container_name" > /dev/null 2>&1; then
		if $(docker inspect -f '{{.State.Status}}' "$container_name" | grep -q "running"); then
    		echo "stopping $container_name container..."
        docker stop "$container_name"
    fi
    echo "removing $container_name container..."
    docker rm "$container_name"
  fi
	docker run -itd \
	                --net=hadoop \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                irm/hadoop-cluster-base
	i=$(( $i + 1 ))
done 



# start hadoop master container
echo "start hadoop-master container..."
container_name="hadoop-master"
if docker inspect "$container_name" > /dev/null 2>&1; then
		if $(docker inspect -f '{{.State.Status}}' "$container_name" | grep -q "running"); then
    		echo "stopping $container_name container..."
        docker stop "$container_name"
    fi
    echo "removing $container_name container..."
    docker rm "$container_name"
  fi
docker run -itd \
                --net=hadoop \
                -p 50070:50070 \
                -p 8088:8088 \
                -p 18080:18080 \
                --name hadoop-master \
                --hostname hadoop-master \
				-v $PWD/data:/data \
                irm/hadoop-cluster-master

echo "Making jobs. Please wait"

#while [ ! -d data/locations_most_actives ]
#do
#  sleep 10
#  #echo "Waiting..."
#done

#echo "Stoping cluster..."
#docker stop hadoop-master

#i=1
#while [ $i -lt $N ]
#do
#	docker stop hadoop-slave$i
#	
#	i=$(( $i + 1 ))
#done                

# get into hadoop master container
#docker exec -it hadoop-master bash