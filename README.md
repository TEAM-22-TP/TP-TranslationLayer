# TP-TranslationLayer

## Dependencies
pip install asyncua aiomqtt

## Run MQTT broker (simulation)
- mosquitto.conf - config file for test mqtt broker
docker run --rm --name mqtt -p 1883:1883 eclipse-mosquitto:2

## Run translation layer
python src/main.py