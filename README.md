[ENG]:

This "toy" data warehouse is built on the following stack:

- DB: **Oracle**
- ETL: **Apache Airflow**

There are four layers in this data warehouse:

- **DMIN** Load Layer – It is assumed that data from source systems arrive here with minimal transformations;
- **STG** Intermediate Layer – In this layer, data undergo certain transformations (in the case of my database, this transformation is a simple join of the main table with a reference table) and are represented for a limited period (day, week, etc.);
- **DM** Data Mart Layer – This layer stores data for the entire period (or a period limited by DWH rules). Data are loaded incrementally or by full reload from the STG;
- **DMOUT** Output Area Layer – This layer is designed for consumer table reads and can be a full copy, snapshot, or view from the DM layer tables.

The simplest and shortest ETL path from the DMIN load layer to the DMOUT output area in my implementation is represented by **two DAGs**.

1. The first DAG loads data from the DMIN layer to the STG layer, and then, with possible transformations, loads data into the DM layer using the **P_UPLOAD_MERGE universal procedure**.
2. The second DAG loads data from the DM layer to the DMOUT layer, using a full reload and preferably a "1 to 1 or by data slice by date" approach, but without complex filtering by any other attributes.


The table structures, DAG code, and the P_UPLOAD_MERGE procedure code can be found in the rest of the file structure of the PET project.


---


[RUS]

Данное «игрушечное» хранилище данных строится на следующем стэке:

- СУБД: **Oracle**
- ETL-инструмент: **Apache Airflow**


Всего имеется 4 слоя в данном хранилище:

- Слой загрузки **DMIN** – предполагается, что туда попадают данные из систем-источников с минимальными преобразованиями;
- Промежуточный слой **STG** – в этот слой данные попадают с определёнными преобразованиями (в случае моей базы этим преобразованием является простое соединение основной таблицы с таблицей-справочником) и представлены данными за ограниченный промежуток времени (день, неделя и т.д.);
- Слой данных витрины (**DM**) – этот слой хранит в себе данные за полный период (или период, ограниченный правилами DWH). Данные в него попадают инкрементально или полной перегрузкой из STG;
- Слой области выгрузки **DMOUT** – этот слой предназначен для чтения таблиц потребителями и может являться полной копией, срезом или view из таблиц слоя DM.

Самый простой и короткий путь ETL от слоя загрузки DMIN до слоя области выгрузки DMOUT в моей реализации представлен двумя DAG’ами.

1. Первый даг загружает данные из слоя DMIN в слой STG, после чего с возможными преобразованиями загружает данные в слой DM с помощью **универсальной процедуры P_UPLOAD_MERGE**.
2. Второй даг загружает данные из слоя DM в слой DMOUT, используя полную перегрузку и предпочтительно льёт **1 в 1 или за срез данных по дате**, но без сложной фильтрации по каким-либо другим признакам.

Непосредственно структуры таблиц, код DAG’ов и код процедуры P_UPLOAD_MERGE можно найти в остальной файловой структуре PET-проекта.




