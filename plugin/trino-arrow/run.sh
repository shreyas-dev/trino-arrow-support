mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo "mvn build failed"
    exit 1
fi
docker stop trino
docker rm trino
docker run --name trino -d -p 8080:8080\
      --volume $PWD/target/trino-arrow-408-SNAPSHOT:/usr/lib/trino/plugin/arrow\
      --volume $PWD/src/main/resources/arrow.properties:/etc/trino/catalog/arrow.properties\
      --volume $PWD/src/main/resources/user_data:/opt/arrow/users.db/user_data\
      --volume $PWD/src/main/resources/employee_parquet:/opt/arrow/users.db/employee_parquet\
      --volume $PWD/src/main/resources/employees:/opt/arrow/users.db/employees\
      --volume $PWD/src/main/resources/some_event:/opt/arrow/users.db/some_event\
      --volume $PWD/jvm.config:/etc/trino/jvm.config\
      -e ARROW_URI=file:/opt/arrow trinodb/trino
