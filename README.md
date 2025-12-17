# TP-TranslationLayer

## Dependencies
pip install asyncua aiomqtt

## Run MQTT broker (simulation)
docker run --rm --name mqtt -p 1883:1883 eclipse-mosquitto:2

## Run translation layer
python src/main.py