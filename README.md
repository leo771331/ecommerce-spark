# Ecommerce Spark Sessionization

## Overview

This repository contains a Spark application for processing ecommerce user activity logs and calculating Weekly Active Users.

Target dataset:

- 2019-Oct.csv
- 2019-Nov.csv
- Kaggle dataset: Ecommerce behavior data from multi category store

## Main Requirements

- Process ecommerce user activity logs with Spark
- Convert event_time from UTC to KST
- Create daily partitions based on KST
- Generate a new session ID when the gap between events of the same user is 5 minutes or more
- Store processed data as Parquet with Snappy compression
- Design Hive External Table
- Support additional period processing
- Include recovery strategy for batch failures
- Calculate WAU by user_id
- Calculate WAU by generated session_id

## Current Status

Initial project structure has been created.

Implementation, SQL, execution scripts, verification results, and AI usage notes will be added step by step.
