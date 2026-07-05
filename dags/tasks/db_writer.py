import os
from tasks.db_connector import DBConnector

def write_to_postgres(config, **kwargs):
    ti = kwargs['ti']
    evaluated_data = ti.xcom_pull(task_ids='evaluate_business_logic', key='evaluated_data')
    dag_id = config['dag_id']
    
    if not evaluated_data:
        return
        
    db = DBConnector()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    for ac in evaluated_data:
        eval_time = ac['eval_time']
        status = ac['status']
        
        if status in ['NEW_ENTRY', 'RE_ENTRY_ALERT']:
            last_alert_time = eval_time
            last_seen_time = eval_time
        elif status == 'OUTSIDE':
            last_alert_time = None
            last_seen_time = None 
        else:
            last_alert_time = None
            last_seen_time = eval_time
            
        cursor.execute("""
            INSERT INTO aircraft_states (hex, dag_id, registration, callsign, status, last_seen_time, last_alert_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hex, dag_id) DO UPDATE SET
                registration = EXCLUDED.registration,
                callsign = EXCLUDED.callsign,
                status = EXCLUDED.status,
                last_seen_time = CASE WHEN EXCLUDED.status = 'OUTSIDE' THEN aircraft_states.last_seen_time ELSE EXCLUDED.last_seen_time END,
                last_alert_time = CASE WHEN EXCLUDED.last_alert_time IS NOT NULL THEN EXCLUDED.last_alert_time ELSE aircraft_states.last_alert_time END
        """, (ac['hex'], dag_id, ac['registration'], ac['callsign'], status, last_seen_time, last_alert_time))
        
    conn.commit()
    cursor.close()
    conn.close()
