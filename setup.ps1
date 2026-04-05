# setup.ps1
param([string]$Action = "up")

switch ($Action) {
    "up"      { docker compose up -d }
    "down"    { docker compose down -v }
    "init" {
        docker compose exec airflow-webserver airflow db migrate
        docker compose exec airflow-webserver airflow users create `
            --username admin --firstname Admin --lastname User `
            --role Admin --email admin@example.com --password admin 2>$null
        Write-Host "✅ Airflow initialized. Login: admin / admin" -ForegroundColor Green
    }
    "logs"    { docker compose logs -f }
    default   { Write-Host "Usage: .\setup.ps1 -Action [up|down|init|logs]" -ForegroundColor Yellow }
}