# Мой опыт в greenplum 
## 1. Необходимо создать таблицы в Greenplum в схеме std<номер пользователя> со структурами следующих таблиц из базы Postgres: 
- gp.plan 
- gp.sales 
 
Для таблиц фактов необходимо определить поля и задать параметры: ориентацию таблиц, сжатие, правила дистрибуции и задать партиционирование, если это необходимо.  

### 1. Необходимо создать таблицы в Greenplum в схеме std<номер пользователя> со структурами следующих файлов CSV: 
- price 
- chanel 
- product 
-region

Для таблицы справочников данные необходимо будет реплицировать на каждый сегмент.

![screen1](https://sun9-37.userapi.com/s/v1/ig2/MtVyvJ-vGDApGb9cxrujmo6YT-1V-DpviveYCaQKrd4CduZqXHIIwRm8zlpbu9NW4WHTuigj2OZmiDX8sBUfVKeM.jpg?quality=95&as=32x39,48x58,72x87,108x131,160x193,240x290,360x435,480x580,540x653,640x774,720x870,1059x1280&from=bu&u=-Pghxc5xzzkqBdjmPCe4A7Eq3t9ssKJnKgXmhIy3k5M&cs=1059x1280)

## [Код для создания таблиц](https://github.com/darwinqqq/greenplum/blob/master/function/create_table.sql)
## 2.Необходимо создать внешние таблицы в Greenplum c использованием протокола PXF и gpfdist
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

# 3."Пользовательские функции"
## 1. Создайте 2 пользовательские функции для загрузки данных в созданные таблицы: 
- 1.Загрузка данных в целевые таблицы должна производиться из внешних EXTERNAL таблиц.
- 2.Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
- 3.Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).
- 4.Для таблиц фактов можно реализовать загрузку следующими способами:
   - DELTA_PARTITION - полная подмена партиций.
   - DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.
## [Код для full загрузки](https://github.com/darwinqqq/greenplum/blob/master/function/f_full.sql)

## [Код для delta_partition](https://github.com/darwinqqq/greenplum/blob/master/function/f_delta_partition.sql)


## 2. Создайте пользовательскую функцию в схеме std <номер студента> для расчёта витрины, которая будет содержать результат выполнения плана продаж в разрезе: 
- Код "Региона".
- Код "Товарного направления" (matdirec).
- Код "Канала сбыта".
- Плановое количество.
- Фактические количество.
- Процент выполнения плана за месяц.
- Код самого продаваемого товара в регионе*.
# Требования к функции по расчёту витрины:

- 1.Функция должна принимать на вход месяц, по которому будут вестись расчеты. 
- 2.Таблица должна формироваться в схеме std <номер студента>.
- 3.Название таблицы должно формироваться по шаблону plan_fact_<YYYYMM>, где <YYYYMM> - месяц расчета. 
- 4.Функция должна иметь возможность безошибочного запуска несколько раз по одному и тому же месяцу. 

## [Код для создания витрины](https://github.com/darwinqqq/greenplum/blob/master/function/f_mart.sql)

## [Код для логов](https://github.com/darwinqqq/greenplum/blob/master/function/f_load_write_log.sql)
