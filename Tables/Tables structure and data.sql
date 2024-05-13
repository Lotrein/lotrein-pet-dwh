-- СУБД - Oracle
  
  -- Схемы:
        -- DMINDS_DATA - схема для данных, загруженных из систем-источников в формате "1 в 1" или с минимальными изменениями
        -- DMDS_STG - промежуточный слой витринных данных, куда происходит загрузка из DMINDS_DATA
        -- DMDS_DATA - схема витринных данных после обработки из STG-слоя
        -- DMOUTDS_DATA - область выгрузки данных для потребителей

  -- Таблицы:
        -- DMINDS_DATA.CLIENT_HIST - таблица с клиентами в историческом формате в слое DMIN
        -- DMINDS_DATA.CLIENT_TYPE_DICT - словарь с типами клиентов
        -- DMDS_STG.CLIENT_DMDELTA - промежуточная таблица, содержащая данные по инкрементальному отбору
        -- DMDS_DATA.CLIENT_HIST - таблица с клиентами в историческом формате в слое DM, заполняемая из DMDELTA
        -- DMOUTDS_DATA.CLIENT_HIST__ADV - таблица с клиентами, предназначенная для чтения системами-потребителями

  -- DDL
    -- DMINDS_DATA.CLIENT_HIST
        create table DMINDS_DATA.CLIENT_HIST
        (
          src_client_id  NUMBER not null,
          dwh_client_id  NUMBER,
          client_type_id NUMBER not null,
          first_name     VARCHAR2(64) not null,
          second_name    VARCHAR2(64),
          middle_name    VARCHAR2(64),
          valid_from     DATE not null,
          valid_to       DATE,
          job_update     NUMBER not null,
          job_insert     NUMBER not null
        )
        tablespace DMINDS_DATA;
        -- Add comments to the table 
        comment on table DMINDS_DATA.CLIENT_HIST
          is 'Таблица клиентов';
        -- Add comments to the columns 
        comment on column DMINDS_DATA.CLIENT_HIST.src_client_id
          is 'Идентификатор клиента в системе-источнике';
        comment on column DMINDS_DATA.CLIENT_HIST.dwh_client_id
          is 'Идентификатор клиента в DWH';
        comment on column DMINDS_DATA.CLIENT_HIST.client_type_id
          is 'Тип клиента';
        comment on column DMINDS_DATA.CLIENT_HIST.first_name
          is 'Имя клиента';
        comment on column DMINDS_DATA.CLIENT_HIST.second_name
          is 'Фамилия клиента';
        comment on column DMINDS_DATA.CLIENT_HIST.middle_name
          is 'Отчество клиента';
        comment on column DMINDS_DATA.CLIENT_HIST.valid_from
          is 'Техническая дата начала действия записи';
        comment on column DMINDS_DATA.CLIENT_HIST.valid_to
          is 'Техническая дата окончания действия записи';
        comment on column DMINDS_DATA.CLIENT_HIST.job_update
          is 'Идентификатор джоба, обновившего запись';
        comment on column DMINDS_DATA.CLIENT_HIST.job_insert
          is 'Идентификатор джоба, добавившего запись';
        -- Create/Recreate indexes 
        create unique index DMINDS_DATA.PK_DWH_CLIENT_ID on DMINDS_DATA.CLIENT_HIST (DWH_CLIENT_ID);

    --  DMINDS_DATA.CLIENT_TYPE_DICT
        create table DMINDS_DATA.CLIENT_TYPE_DICT
        (
          client_type_id   NUMBER,
          client_type_name VARCHAR2(64),
          created_at       DATE
        )
        tablespace DMINDS_DATA;
        -- Add comments to the table 
        comment on table DMINDS_DATA.CLIENT_TYPE_DICT
          is 'Справочник типов клиентов';
        -- Add comments to the columns 
        comment on column DMINDS_DATA.CLIENT_TYPE_DICT.client_type_id
          is 'Идентификатор типа клиента';
        comment on column DMINDS_DATA.CLIENT_TYPE_DICT.client_type_name
          is 'Наименование типа клиента';
        comment on column DMINDS_DATA.CLIENT_TYPE_DICT.created_at
          is 'Дата добавления типа клиента';


    --  DMDS_STG.CLIENT_DMDELTA
        create table DMDS_STG.CLIENT_DMDELTA
        (
          src_client_id NUMBER not null,
          dwh_client_id NUMBER,
          client_type   VARCHAR2(64) not null,
          first_name    VARCHAR2(64) not null,
          second_name   VARCHAR2(64),
          middle_name   VARCHAR2(64),
          valid_from    DATE not null,
          valid_to      DATE,
          job_update    NUMBER not null,
          job_insert    NUMBER not null,
          as_of_day     DATE,
          operation_day DATE
        )
        tablespace DMDS_STG;
        -- Add comments to the table 
        comment on table DMDS_STG.CLIENT_DMDELTA
          is 'Таблица клиентов для потребителя ADS';
        -- Add comments to the columns 
        comment on column DMDS_STG.CLIENT_DMDELTA.src_client_id
          is 'Идентификатор клиента в системе-источнике';
        comment on column DMDS_STG.CLIENT_DMDELTA.dwh_client_id
          is 'Идентификатор клиента в DWH';
        comment on column DMDS_STG.CLIENT_DMDELTA.client_type
          is 'Тип клиента';
        comment on column DMDS_STG.CLIENT_DMDELTA.first_name
          is 'Имя клиента';
        comment on column DMDS_STG.CLIENT_DMDELTA.second_name
          is 'Фамилия клиента';
        comment on column DMDS_STG.CLIENT_DMDELTA.middle_name
          is 'Отчество клиента';
        comment on column DMDS_STG.CLIENT_DMDELTA.valid_from
          is 'Техническая дата начала действия записи';
        comment on column DMDS_STG.CLIENT_DMDELTA.valid_to
          is 'Техническая дата окончания действия записи';
        comment on column DMDS_STG.CLIENT_DMDELTA.job_update
          is 'Идентификатор джоба, обновившего запись';
        comment on column DMDS_STG.CLIENT_DMDELTA.job_insert
          is 'Операционная дата';
        comment on column DMDS_STG.CLIENT_DMDELTA.as_of_day
          is 'Фактическая дата записи';


    --  DMDS_DATA.CLIENT_HIST
        create table DMDS_DATA.CLIENT_HIST
        (
          src_client_id NUMBER not null,
          dwh_client_id NUMBER,
          client_type   VARCHAR2(64) not null,
          first_name    VARCHAR2(64) not null,
          second_name   VARCHAR2(64),
          middle_name   VARCHAR2(64),
          valid_from    DATE not null,
          valid_to      DATE,
          job_update    NUMBER not null,
          job_insert    NUMBER not null,
          as_of_day     DATE,
          operation_day DATE
        )
        tablespace DMDS_DATA;
        -- Add comments to the table 
        comment on table DMDS_DATA.CLIENT_HIST
          is 'Таблица клиентов для потребителя ADS';
        -- Add comments to the columns 
        comment on column DMDS_DATA.CLIENT_HIST.src_client_id
          is 'Идентификатор клиента в системе-источнике';
        comment on column DMDS_DATA.CLIENT_HIST.dwh_client_id
          is 'Идентификатор клиента в DWH';
        comment on column DMDS_DATA.CLIENT_HIST.client_type
          is 'Тип клиента';
        comment on column DMDS_DATA.CLIENT_HIST.first_name
          is 'Имя клиента';
        comment on column DMDS_DATA.CLIENT_HIST.second_name
          is 'Фамилия клиента';
        comment on column DMDS_DATA.CLIENT_HIST.middle_name
          is 'Отчество клиента';
        comment on column DMDS_DATA.CLIENT_HIST.valid_from
          is 'Техническая дата начала действия записи';
        comment on column DMDS_DATA.CLIENT_HIST.valid_to
          is 'Техническая дата окончания действия записи';
        comment on column DMDS_DATA.CLIENT_HIST.job_update
          is 'Идентификатор джоба, обновившего запись';
        comment on column DMDS_DATA.CLIENT_HIST.job_insert
          is 'Операционная дата';
        comment on column DMDS_DATA.CLIENT_HIST.as_of_day
          is 'Фактическая дата записи';
        -- Create/Recreate indexes 
        create unique index DMDS_DATA.PK_DWH_CLIENT_ID on DMDS_DATA.CLIENT_HIST (DWH_CLIENT_ID, VALID_FROM);


    --  DMDS_DATA.CLIENT_HIST
        create table DMOUTDS_DATA.CLIENT_HIST__ADV
        (
          src_client_id NUMBER not null,
          client_type   VARCHAR2(64) not null,
          client_name   VARCHAR2(256) not null,
          valid_from    DATE not null,
          valid_to      DATE,
          as_of_day     DATE,
          operation_day DATE
        )
        tablespace DMDS_DATA;
        -- Add comments to the table 
        comment on table DMOUTDS_DATA.CLIENT_HIST__ADV
          is 'Таблица клиентов для потребителя ADS';
        -- Add comments to the columns 
        comment on column DMOUTDS_DATA.CLIENT_HIST__ADV.src_client_id
          is 'Идентификатор клиента в системе-источнике';
        comment on column DMOUTDS_DATA.CLIENT_HIST__ADV.client_type
          is 'Тип клиента';
        comment on column DMOUTDS_DATA.CLIENT_HIST__ADV.client_name
          is 'Имя клиента';
        comment on column DMOUTDS_DATA.CLIENT_HIST__ADV.valid_from
          is 'Техническая дата начала действия записи';
        comment on column DMOUTDS_DATA.CLIENT_HIST__ADV.valid_to
          is 'Техническая дата окончания действия записи';
        comment on column DMOUTDS_DATA.CLIENT_HIST__ADV.as_of_day
          is 'Фактическая дата записи';
        -- Create/Recreate indexes 
        create unique index DMOUTDS_DATA.PK_SRC_CLIENT_ID on DMOUTDS_DATA.CLIENT_HIST__ADV (SRC_CLIENT_ID);


  -- DML
    -- DMINDS_DATA.CLIENT_HIST
    insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (21, 121, 3, 'Антон', 'Петров', 'Николаевич', to_date('19-04-2024', 'dd-mm-yyyy'), null, 1, 1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (1, 101, 1, 'Алексей', 'Иванов', 'Николаевич', to_date('01-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (2, 102, 1, 'Мария', 'Петрова', 'Александровна', to_date('02-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (3, 103, 2, 'ООО "Рога и Копыта"', null, null, to_date('03-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (4, 104, 3, 'Сергей', 'Сидоров', 'Петрович', to_date('04-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (5, 105, 2, 'ЗАО "Промышленные Решения"', null, null, to_date('05-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (6, 106, 3, 'Екатерина', 'Васильева', 'Дмитриевна', to_date('06-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (7, 107, 2, 'ООО "Инновационные Технологии"', null, null, to_date('07-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (8, 108, 1, 'Дмитрий', 'Морозов', 'Андреевич', to_date('08-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (9, 109, 2, 'АО "БыстроСтрой"', null, null, to_date('09-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (10, 110, 1, 'Наталья', 'Чернова', 'Игоревна', to_date('10-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (11, 111, 2, 'ООО "Фармация"', null, null, to_date('11-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (12, 112, 3, 'Виктор', 'Кузнецов', 'Леонидович', to_date('12-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (13, 113, 2, 'ООО "Сетевые Решения"', null, null, to_date('13-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (14, 114, 1, 'Ирина', 'Белова', 'Максимовна', to_date('14-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (15, 115, 2, 'ЗАО "ГлобалКонструкции"', null, null, to_date('15-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (16, 116, 1, 'Андрей', 'Широков', 'Валерьевич', to_date('16-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (17, 117, 2, 'ООО "ДелоТех"', null, null, to_date('17-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (18, 118, 1, 'Татьяна', 'Ковалева', 'Анатольевна', to_date('18-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (19, 119, 2, 'ООО "РетроСтиль"', null, null, to_date('19-04-2024', 'dd-mm-yyyy'), null, -1, -1);
insert into DMINDS_DATA.CLIENT_HIST (src_client_id, dwh_client_id, client_type_id, first_name, second_name, middle_name, valid_from, valid_to, job_update, job_insert)
values (20, 120, 3, 'Олег', 'Захаров', 'Сергеевич', to_date('20-04-2024', 'dd-mm-yyyy'), null, -1, -1);
commit;

    -- DMINDS_DATA.CLIENT_TYPE_DICT
    insert into DMINDS_DATA.CLIENT_TYPE_DICT (client_type_id, client_type_name, created_at)
values (1, 'Физическое лицо', to_date('25-04-2024 20:27:49', 'dd-mm-yyyy hh24:mi:ss'));
insert into DMINDS_DATA.CLIENT_TYPE_DICT (client_type_id, client_type_name, created_at)
values (2, 'Юридическое лицо', to_date('25-04-2024 20:27:49', 'dd-mm-yyyy hh24:mi:ss'));
insert into DMINDS_DATA.CLIENT_TYPE_DICT (client_type_id, client_type_name, created_at)
values (3, 'Знакомое лицо', to_date('25-04-2024 20:28:04', 'dd-mm-yyyy hh24:mi:ss'));
commit;

    -- Остальные таблицы могут быть пустыми на первоначальном этапе, т.к. будут заполняться в процессе ETL-загрузок