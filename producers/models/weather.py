"""Methods pertaining to weather data"""
import json
import logging
import random
from enum import IntEnum
from pathlib import Path

import requests
from confluent_kafka import avro

from .producer import Producer
from ..config import KAFKA_REST_PROXY_URL

logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""
    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = KAFKA_REST_PROXY_URL

    key_schema = None
    value_schema = None

    winter_months = {0, 1, 2, 3, 10, 11}
    summer_months = {6, 7, 8}

    def __init__(self, month):
        super().__init__(
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        response = requests.post(
            url=f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={
                "Content-Type": "application/vnd.kafka.avro.v2+json",
                "Accept": "application/vnd.kafka.v2+json"
            },
            data=json.dumps({
                "key_schema": Weather.key_schema,
                "value_schema": Weather.value_schema,
                "records": [
                    {
                        "key": self.time_millis(),
                        "value": {
                            "temperature": self.temp,
                            "status": self.status.name,
                        }
                    }
                ]
            })
        )
        response.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
