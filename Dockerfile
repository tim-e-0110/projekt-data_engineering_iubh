FROM bitnami/spark:3.5.5
USER root
WORKDIR /app
COPY ./requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY ./src/daily_aggregator.py /app/
COPY ./src/test_db.py /app/
COPY ./drivers/postgresql-*.jar /opt/bitnami/spark/jars/
ENTRYPOINT ["spark-submit"]
CMD ["/app/daily_aggregator.py"]