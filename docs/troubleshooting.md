### ⚠️ Dependency Warning: aiobotocore vs botocore
- Airflow 2.8.0 pre-installs `aiobotocore` for async S3 operations
- Our project uses synchronous `boto3`, so the version conflict is harmless
- For production: pin compatible versions in requirements or build custom Airflow image