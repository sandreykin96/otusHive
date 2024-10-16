Скачиваем данные по проданным машинам со стран персидского залива с [Ссылка в kaggle](https://www.kaggle.com/datasets/willianoliveiragibin/cars-yallamotors?resource=download)

```ls```
–hadoop-2.7.4  hive

Создаем директорию для хранения данных по авто

```hdfs dfs -mkdir /user/hive/warehouse/cars```

Копируем в hdfs.
```hdfs dfs -put cars.csv /user/hive/warehouse/cars ```

Проверяем сработало ли
```hdfs dfs -ls /user/hive/warehouse/cars```
Found 1 items
-rw-r--r--   3 root supergroup     547376 2024-10-14 21:21 /user/hive/warehouse/cars/cars.csv

Переходим в хайв
```hive```

Убеждаемся что все работает
hive> ```show databases;```
OK

hive> ```show tables;```
hive> ```create database otus;```

Создаем БД otus
hive> ```create database otus;```

Переходим в dbeaver, видимо что БД действительно существует 

Создаем таблицу с автомобилями.
```sql
CREATE EXTERNAL TABLE otus.cars
      (car_name STRING,
       price STRING,
       engine_capacity int,
       cylinder int,
       horse_power int,
       top_speed int,
       seats STRING, 
       brand STRING,
       country STRING
      )
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES (
       "separatorChar" = ","
      ) 
      STORED AS TEXTFILE
      LOCATION '/user/hive/warehouse/cars'
      tblproperties("skip.header.line.count"="1"
      );

select * from otus.cars;
```
Создадим таблицу которая хранит 10 самых мощных автомобилей от марки Aston Martin

```sql
CREATE TABLE IF NOT EXISTS powerfull_aston_martin_cars AS 
select * from otus.cars
WHERE brand = 'aston-martin'  
ORDER BY horse_power desc
 LIMIT 10
```

Убедимся что все создано корректно 
```sql
select * from default.powerfull_aston_martin_cars
```

Создадим витрину которая будет отображать бренды выпускающие в среднем самые быстрые автомобили 
```sql
CREATE TABLE IF NOT EXISTS average_top_speed_brands AS 
select brand, AVG(top_speed) as avg_top_speed from otus.cars 
GROUP BY brand
HAVING  avg_top_speed > 200
ORDER BY avg_top_speed DESC  

select * from default.average_top_speed_brands
LIMIT 5
```

Получаем любопытный ответ


| Марка | Средняя скорость |
| --- | --- |
| ferrari | 419.375 |
| bugatti | 350.0 |
| tesla | 335.44444444444446 |
| lamborghini | 334.7142857142857 |
| mclaren | 329.7741935483871 |

Создадим витрину которая будет содержать количество проданных машин в разрезе брендов
```sql
CREATE TABLE IF NOT EXISTS sold_cars_by_brands AS 
select brand, COUNT(*)  as count_of_cars from otus.cars 
GROUP BY brand
ORDER BY count_of_cars DESC  
```

Витрина которая покажет самые популярные бренды в катаре

```sql
CREATE TABLE IF NOT EXISTS top_brands_qatar AS 
select brand, country,  COUNT(*)  as count_of_cars from otus.cars 
WHERE  country  = 'qatar'
GROUP BY brand, country 
ORDER BY count_of_cars DESC
LIMIT 5

SELECT  * FROM top_brands_qatar
```
Видим то что в топе немецкая тройка, а также 2 бренда из Японии.
mercedes-benz	qatar	99
bmw	qatar	87
audi	qatar	79
toyota	qatar	54
nissan	qatar	53
|Марка     |Страна | Количество проданных машин|
| --- | --- | --- |
|mercedes-benz|	qatar	|99|
|bmw	|qatar	|87|
|audi	|qatar	|79|
|toyota|	qatar	|54|
|nissan|	qatar	|53|

Создадим витрину которая показывает бренды продававшие машины с количеством цилиндров меньше четырех
```sql
CREATE TABLE IF NOT EXISTS less_than_three_cylynder_cars AS 
select brand, cylinder,  COUNT(*) from otus.cars 
WHERE cylinder < 4
GROUP  BY brand, cylinder 
```
Отмечаем то что Geely продало целых 4 автомобиля с количеством цилиндров меньше четырех
|Модель     |Количество цилиндров | Количество проданных машин|
| --- | --- | --- |
| audi	| 3 |	25 |
| bmw	      | 3 | 19 | 
| citroen	| 3 |	6  |
|ds         | 3 | 2  |
|ford 	|3  |	8  |
|geely	|3  | 4  |

| Command | Description |
| --- | --- |
| git status | List all new or modified files |
| git diff | Show file differences that haven't been staged |
