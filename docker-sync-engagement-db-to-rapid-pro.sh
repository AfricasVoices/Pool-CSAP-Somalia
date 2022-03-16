#!/bin/bash

set -e

IMAGE_NAME="$(<configurations/docker_image_name.txt)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --incremental-cache-volume)
            INCREMENTAL_ARG="--incremental-cache-path /cache"
            INCREMENTAL_CACHE_VOLUME_NAME="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 3 ]]; then
    echo "Usage: $0
    [--incremental-cache-volume <incremental-cache-volume>]
    <user> <google-cloud-credentials-file-path> <configuration-module>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
CONFIGURATION_MODULE=$3

CMD="pipenv run python -u sync_engagement_db_to_rapid_pro.py ${INCREMENTAL_ARG} ${USER} \
    /credentials/google-cloud-credentials.json ${CONFIGURATION_MODULE}"

if [[ "$INCREMENTAL_ARG" ]]; then
    container="$(docker container create -w /app --mount source="$INCREMENTAL_CACHE_VOLUME_NAME",target=/cache "$IMAGE_NAME" /bin/bash -c "$CMD")"
else
    container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
fi

echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_PATH -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Tear down the container when it has run successfully
docker container rm "$container" >/dev/null
