#!/bin/bash
python wait_for_kafka.py
python wait_for_postgres.py
python consumer.py