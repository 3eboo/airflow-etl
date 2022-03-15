build:
	chmod +x docker/docker-airflow/pg-init-scripts/init-user-db.sh
	docker build --rm --force-rm -t docker-airflow-spark:1.10.7_3.1.2 docker/docker-airflow/
	cd docker && docker-compose up --build -d


