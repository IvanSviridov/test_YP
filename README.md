# test_YP
1) git clone https://github.com/IvanSviridov/test_YP.git  
Открываем проект. Далее в  терминале (команды указывал для windows).  
2) Смотрим есть ли запущенные контейнеры: docker ps  
3) По необходимости останавливаем их: FOR /f "tokens=*" %i IN ('docker ps -q') DO docker stop %i  
4) Запускаем docker-airflow: docker-compose -f docker-compose-CeleryExecutor.yml up -d  
5) Смотрим запущенные контейнеры, а таже id контейнера с постгрессом: docker ps . Например, 36ac863eb19c postgres:9.6.  
6) Находим ip контейнера с постгрессом по id из предыдущего пункта: docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" 36ac863eb19c . Например, получаем 172.20.0.3  
7) Заходим на localhost и переходим в раздел коннекторов: http://localhost:8080/admin/connection/  
Либо изменяем, либо создаем conn_id:  
Conn Id:  database_PG  
Conn Type:  Postgres  
Host:  172.20.0.3 (указываем ip postgres)   
Schema:  
Login:  airflow  
Password:  airflow  
Port:   5432  
![image](https://user-images.githubusercontent.com/85152099/161455074-0a481c06-6901-4ce0-b13d-e67f585b28a5.png)
8) На этом все. Переводим dag в положение 'on' http://localhost:8080/admin/.  
Переходим в лог последней таски и смотрим:
![image](https://user-images.githubusercontent.com/85152099/161455202-c70d8d69-cd0d-41a9-8e3e-7d074a498ae6.png)
![image](https://user-images.githubusercontent.com/85152099/161455244-ff39b607-5888-4ff2-bb21-25dfbf94bd89.png)
![image](https://user-images.githubusercontent.com/85152099/161455250-402de7c6-0824-45e8-9e43-db5566be6dd5.png)
Актуальный курс добавился в таблицу rates. Ура!!!)  
9) Чтобы вывести исторические данные надо перейти в test_YP/dags/api.py. И меняем один из параметров:  
![image](https://user-images.githubusercontent.com/85152099/161455463-58831577-3a88-4098-83bf-167e9392fab4.png)  
Просьба быть аккуратным, так как курс биткойна к доллару есть не на всем интервале времени.  
Финальная таблица имеет поля: 
query_from - базовая валюта  
query_to - минорная валюта  
info_rate - курс  
date - дата из источника (дата актуальности курса)  
processed_dttm - дата и время выгрузки (до минуты)  
По бизнес логике даг разделен на 4 таски: extract_data, transform_data, create_table, insert_into_table. 
По умолчанию грузит актуальный курс каждые три часа, в случае необходимости есть возможность подгрузки исторических данных (см. выше)  
Но хотел бы обратить внимание, что Apache Airflow — это оркестратор, а не ETL-инструмент, т.е. он предназначен для управления задачами, а не для передачи данных. Поэтому не используйте встроенный инструмент XCom, если данные имеют большой размер. Но, так как максимальный размер исторических данных 366 дней(из документации к api: https://exchangerate.host/#/#articles, то тут это никак не скажется)))  
Airflow поднимал на основе docker-airflow. Выбрал celery executor, что наиболее близко к боевой задаче. Соответственно поднимаются контейнеры:  
- Собственно Airflow: Scheduler, Webserver. Там же будет крутится Flower для мониторинга Celery-задач (потому что его уже затолкали в apache/airflow:1.10.10-python3.7);   
- PostgreSQL, в который Airflow будет писать свою служебную информацию (данные планировщика, статистика выполнения и т. д.), а Celery — отмечать завершенные таски;
- Redis, который будет выступать брокером задач для Celery;  
- Celery worker, который и займется непосредственным выполнением задачек.  
