# Мой опыт в greenplum 
## 1. Необходимо создать таблицы в Greenplum со структурами следующих таблиц из базы Postgres: 
- gp.plan 
- gp.sales 
 
> Для таблиц фактов необходимо определить поля и задать параметры: ориентацию таблиц, сжатие, правила дистрибуции и задать партиционирование, если это необходимо. 

## 2. Необходимо создать таблицы в Greenplum со структурами следующих файлов CSV: 
- [price](https://github.com/darwinqqq/greenplum/blob/master/file/price.csv) 
- [chanel](https://github.com/darwinqqq/greenplum/blob/master/file/chanel.csv) 
- [product](https://github.com/darwinqqq/greenplum/blob/master/file/product.csv) 
- [region](https://github.com/darwinqqq/greenplum/blob/master/file/region.csv) 

>Для таблицы справочников данные необходимо будет реплицировать на каждый сегмент.

![screen1](https://sun9-37.userapi.com/s/v1/ig2/MtVyvJ-vGDApGb9cxrujmo6YT-1V-DpviveYCaQKrd4CduZqXHIIwRm8zlpbu9NW4WHTuigj2OZmiDX8sBUfVKeM.jpg?quality=95&as=32x39,48x58,72x87,108x131,160x193,240x290,360x435,480x580,540x653,640x774,720x870,1059x1280&from=bu&u=-Pghxc5xzzkqBdjmPCe4A7Eq3t9ssKJnKgXmhIy3k5M&cs=1059x1280)

## [Код для создания таблиц](https://github.com/darwinqqq/greenplum/blob/master/function/create_table.sql)
# 3.Необходимо создать внешние таблицы в Greenplum c использованием протокола PXF и gpfdist
### 1. Необходимо создать внешние таблицы в Greenplum c использованием протокола PXF для доступа к данным следующих таблиц базы Postgres: 
- gp.plan 
- gp.sales 
## [Код для pxf](https://github.com/darwinqqq/greenplum/blob/master/function/f_pxf.sql)
### 2. Необходимо создать внешние таблицы в Greenplum c использованием протокола gpfdist для доступа к данным следующих файлов CSV: 
- [price](https://github.com/darwinqqq/greenplum/blob/master/file/price.csv) 
- [chanel](https://github.com/darwinqqq/greenplum/blob/master/file/chanel.csv) 
- [product](https://github.com/darwinqqq/greenplum/blob/master/file/product.csv) 
- [region](https://github.com/darwinqqq/greenplum/blob/master/file/region.csv) 
## [Код для gpfdist](https://github.com/darwinqqq/greenplum/blob/master/function/gpfdist.sql)

# 4."Пользовательские функции"
## 1. Создайте 2 пользовательские функции для загрузки данных в созданные таблицы: 
 - ### 1. Загрузка данных в целевые таблицы должна производиться из внешних EXTERNAL таблиц.
- ### 2.Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
- ### 3.Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой >таблицы и полная вставка всех записей).

- ### 4.Для таблиц фактов можно реализовать загрузку следующими способами:
   - ### DELTA_PARTITION - полная подмена партиций.
  - ### DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из >временной таблицы в целевую.
## [Код для full загрузки](https://github.com/darwinqqq/greenplum/blob/master/function/f_full.sql)

## [Код для delta_partition](https://github.com/darwinqqq/greenplum/blob/master/function/f_delta_partition.sql)


## 2. Создайте пользовательскую функцию для расчёта витрины, которая будет содержать результат выполнения плана продаж в разрезе: 
- Код "Региона".
- Код "Товарного направления" (matdirec).
- Код "Канала сбыта".
- Плановое количество.
- Фактические количество.
- Процент выполнения плана за месяц.
- Код самого продаваемого товара в регионе*.
# Требования к функции по расчёту витрины:

- ### 1.Функция должна принимать на вход месяц, по которому будут вестись расчеты. 
- ### 2.Таблица должна формироваться в схеме std <номер студента>.
- ### 3.Название таблицы должно формироваться по шаблону plan_fact_YYYYMM, где YYYYMM - месяц >расчета. 
- ### 4.Функция должна иметь возможность безошибочного запуска несколько раз по одному и тому же >месяцу. 

## [Код для создания витрины](https://github.com/darwinqqq/greenplum/blob/master/function/f_mart.sql)

## [Код для логов](https://github.com/darwinqqq/greenplum/blob/master/function/f_load_write_log.sql)
# 5.Оркестрация загрузок в aiflow 
### В Apache Airflow необходимо создать:
- 1.Соединение gp_conn с Greenplum 
- Directed Acyclic Graph с названием std11_3_main dag, который будет состоять из следующих Task'ов:
- Цикличная загрузка справочных данных в целевые таблицы из внешних  таблиц. 
- Загрузка данных для целевых таблиц фактов из внешних таблиц.
- Расчёт витрины.
- DAG должен удалённо запускать в Greenplum пользовательские функции.
>- Запустим aiflow в docker с помошью официально образа 
>- Создаём соединение grennpum  в Admin -> Connections
>- Напишем [dag](https://github.com/darwinqqq/greenplum/blob/master/airflow/my_dag.py) для выполнения пользовательских функци и расчёта витрин
# 6.Применение Clickhouse
### 1. Создайте базу данных на 206 хосте.

### 2. Создайте в своей базе данных интеграционную таблицу ch_plan_fact_ext для доступа к данным витрины plan_fact_YYYYMM в системе Greenplum.

### 3. Создайте следующие словари для доступа к данным таблиц системы Greenplum:

- ch_price_dict
- ch_chanel_dict
- ch_product_dict
- ch_region_dict
## [Код для создания словарей в click house](https://github.com/darwinqqq/greenplum/blob/master/function/click%20house.txt)
### 4. Создайте реплицированные таблицы ch_plan_fact на всех хостах кластера. Создайте распределённую таблицу ch_plan_fact_distr, выбрав для неё корректный ключ шардирования. Вставьте в неё все записи из таблицы  ch_plan_fact_ext.
## [Код для создания реплицированных таблиц](https://github.com/darwinqqq/greenplum/blob/master/function/click%20house2.txt)

# 7.Создание дэшборда в Apache Superset
## 1. Создайте соединение ch_std<номер пользователя> с Clickhouse. 

## 2. Создайте датасет (используя свое соединение) ss_plan_fact с помощью собственного SQL запроса к таблице в Clickhouse и с использованием функций для словарей-справочников. SQL запрос должен формировать аналогичное представление, как в Greenplum (v_plan_fact), с результатами выполнения плана продаж, текстами для кодов и информацией о самом продаваемом товаре в регионе.

Клонировал  репозиторий Superset на GitHub
```
git clone --depth=1  https://github.com/apache/superset.git
```
Выполнил в папке с docker обазом
```
touch ./docker/requirements-local.txt

echo "pip install clickhouse-connect" >> ./docker/requirements-local.txt

docker compose -f docker-compose-non-dev.yml up
```
Подключился к click house 
~~~
clickhousedb://{username}:{password}@{hostname}:{port}/{database}
~~~

