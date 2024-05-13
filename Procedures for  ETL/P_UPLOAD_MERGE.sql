-- Процедура используется для загрузка данных из STG-слоя в слой конечных данных витрины (DM)
-- p_upload_merge использует MERGE для вставки новых записей в целевую таблицу и обновления технических данных по уже существующим записям 

-- Входные параметры процедуры:
  -- p_target_owner_name: схема целевой таблицы (DM)
  -- p_target_table_name: имя целевой таблицы (DM)
  -- p_source_table_name: схема таблицы-источника (STG)
  -- p_source_table_name: имя таблицы-источника (DM)
  -- p_debug_mode: вкл/выкл режим дебага (по умолчанию - "выкл")


CREATE OR REPLACE PROCEDURE p_upload_merge(p_target_owner_name VARCHAR2,
                                                          p_target_table_name VARCHAR2,
                                                          p_source_owner_name VARCHAR2,
                                                          p_source_table_name VARCHAR2,
                                                          p_debug_mode        BOOLEAN DEFAULT FALSE)
  AUTHID CURRENT_USER IS

  v_sql                 CLOB;
  v_target_columns_list VARCHAR2(4000);
  v_source_columns_list VARCHAR2(4000);
  v_pk_column_list      VARCHAR2(4000);

BEGIN

  -- Собираем список полей из целевой таблицы, складываем их в переменную v_target_columns_list
  SELECT (WITH target_columns_list AS (SELECT column_name
                                         FROM all_tab_columns a
                                        WHERE a.table_name =
                                              upper(p_target_table_name)
                                          AND a.owner =
                                              upper(p_target_owner_name))
         
           SELECT rtrim(listagg('t.' || column_name || ',
'),
                         ',
')
             FROM target_columns_list)
             INTO v_target_columns_list
             FROM dual;


  IF p_debug_mode = TRUE THEN
  
    dbms_output.put_line('Список полей целевой таблицы: 
' || v_target_columns_list || '
');
  
  END IF;

  -- Собираем список полей из таблицы-источника, складываем их в переменную v_source_columns_list
  SELECT (WITH source_columns_list AS (SELECT column_name
                                         FROM all_tab_columns a
                                        WHERE a.table_name =
                                              upper(p_source_table_name)
                                          AND a.owner =
                                              upper(p_source_owner_name))
         
           SELECT rtrim(listagg('s.' || column_name || ',
'),
                         ',
')
             FROM source_columns_list)
             INTO v_source_columns_list
             FROM dual;


  IF p_debug_mode = TRUE THEN
  
    dbms_output.put_line('Список полей таблицы-источника: 
' || v_source_columns_list || '
');
  
  END IF;

  -- Собираем ключ, по которому должна происходить загрузка данных или их обновление
  SELECT (WITH pk_column_list AS (SELECT a.column_name, a.column_position
                                    FROM all_ind_columns a
                                   WHERE a.table_name =
                                         upper(p_target_table_name)
                                     AND a.table_owner =
                                         upper(p_target_owner_name)
                                     AND a.index_name LIKE 'PK!_%'
                                   ESCAPE '!'
                                   ORDER BY a.column_position)
           SELECT rtrim(listagg('t.' || column_name || ' = s.' ||
                                 column_name,
                                 ' AND
') within GROUP(ORDER BY column_position),
                         'AND
')
             FROM pk_column_list)
             INTO v_pk_column_list
             FROM dual;


  IF p_debug_mode = TRUE THEN
  
    dbms_output.put_line('Список полей, составляющих первичный ключ: 
' || v_pk_column_list || '
');
  
  END IF;

  -- Склеиваем запрос, используя полученные ранее списки полей и ключ
  v_sql := 'MERGE INTO ' || p_target_owner_name || '.' ||
           p_target_table_name || ' t
USING (SELECT * FROM ' || p_source_owner_name || '.' ||
           p_source_table_name || ') s
ON (' || v_pk_column_list || ')
WHEN MATCHED THEN
  UPDATE SET t.job_update = ' ||
           floor(dbms_random.value(1, 1000)) || '
WHEN NOT MATCHED THEN
  INSERT
    (
' || v_target_columns_list || '
)
  VALUES
    (
' || v_source_columns_list || '
)';

  IF p_debug_mode = TRUE THEN
  
    dbms_output.put_line('Скрипт аплоада:
' || v_sql);
  
  END IF;

  -- Выполняем запрос
  EXECUTE IMMEDIATE v_sql;

  IF p_debug_mode = TRUE THEN
  
    dbms_output.put_line(SQL%ROWCOUNT);
  
  END IF;

  COMMIT;

END p_upload_merge;
