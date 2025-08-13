# Devcontainers ETL Node

This project is an example of how to use DevContainers to build a standalone ETL node that integrates multiple data sources in order to provide data to the [devcontainers-consumer-node](https://github.com/marcoscobo/devcontainers-consumer-node) project. Its main purpose is to demonstrate how to develop isolated and reproducible pipelines for complex data processing tasks.

## Project Purpose

The goal of this project is to provide a preconfigured development environment that facilitates the creation of ETL pipelines. This includes:

- **Data Extraction**: Connect to multiple data sources and extract raw data.
- **Data Transformation**: Process and clean data to make it usable for downstream applications.
- **Data Loading**: Load processed data into target systems for further use.

## Key Features

- **DevContainers**: Development container configuration to ensure a consistent environment.
- **Integration with Multiple Data Sources**: Preconfigured connectors for Kafka (message broker), MinIO (object storage), and Postgres (relational database).
- **Python Support**: Optimized environment for developing ETL scripts in Python.
- **Reproducibility**: Ensures consistent results across different environments.

## Project Structure

- `.devcontainer/`: Development container configuration.
  - `devcontainer.json`: Main DevContainer configuration.
  - `docker-compose.yml`: Docker services configuration.
- `src/`: ETL scripts and configurations.
  - `data_chargers/`: Scripts for loading data into target systems such as Kafka, Postgres or MinIO.
  - `etl/`: Scripts for extracting, transforming, and processing data from the sources.

## Prerequisites

- Docker and Docker Compose installed on your machine.
- Visual Studio Code with the DevContainers extension.

## Getting Started

1. Clone this repository to your local machine.
2. Open the project in Visual Studio Code.
3. Open the project in the development container (Cmd+Shift+P --> Dev Containers: Reopen in Container).
4. Start developing your ETL pipelines in the isolated environment.

## License

This project is licensed under the terms of the MIT license.
