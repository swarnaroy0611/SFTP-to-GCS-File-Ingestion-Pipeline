Insert into CONFIG_TABLE(key,value,cod_file) values
('vED_FLAG_END','Y','IN_SFTP'),
('vED_FLAG_START','Y','IN_SFTP'),
('vED_MSG_END','{"SISTEMA": "DEMO", "AREA": "PRO", "BATCH_ID": "#DAG_ID", "START_INTERVAL_TST":"#START_TIME", "END_INT ERVAL TST":"#TST CREAZIONE#", "EVENT_TYPE": "BATCH_RUN", "EVENT_CLASS": "END_RUN", "STATUS": "OK", "RUN_ID":"#RUN_ID"}' ,'IN_SFTP'),
('vED_MSG_START','{"SISTEMA": "DEMO", "AREA": "PRO", "BATCH_ID": "#DAG ID#", "START_INTERVAL_TST":"#TST_CREAZIONE", "EN D INTERVAL TST": "#TST_CREAZIONE", "EVENT_TYPE": "BATCH_RUN", "EVENT_CLASS":"START_RUN", "STATUS": "OK", "RUN_ID":"RUN ID"}','IN_SFTP'),
('vED_PRJ','ED PROJECT','IN_SFTP'),
('vED_TOPIC','Event_Driven_Queue','IN_SFTP'),
('vPython_Script_ED','ed_publish_message.py','IN_SFTP'),
('vREMOTE_PATH','/remote/sample','IN_SFTP'),
('vSFTP_CONN ID','sftp-cons','IN_SFTP'),
('vFILE_NAME','"Sales*.csv"','IN_SFTP'),
('vGCS_CONN_ID','google Connection ID','IN_SFTP'),
('vTMP_PATH','/tmp/IN_SFTP','IN_SFTP'),
('vCLOUD_OUTPUT_PATH','gs://ETL_DEMO_PROJECT/Source','IN_SFTP');