# F1 ELT Pipeline
A data pipeline to extract and create a dashboard about Formula 1.
Since Databricks is only free on Azure, I'll be using Azure's services for this project.

## Motivation
Project is based on a (newfound) interest in Data Engineering and Formula 1.

This project is more complex than required just to learn some extra tools related to data engineering, eg:
1. Databricks
2. dbt
3. Spark
4. Prometheus & Grafana

## Architecture
Architecture is largely inspired by the [databricks article](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture)

## Output

## Notes
1. The current best practice for connecting Databricks to Azure Data Lakes is through Unity Catalog. But because that is only offered on the Premium tier I will be sticking with Service Principals (keys secured with Azure Key-Vault).
