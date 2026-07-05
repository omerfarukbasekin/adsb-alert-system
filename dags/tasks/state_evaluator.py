import os
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from tasks.db_connector import DBConnector

def evaluate_business_logic(config, **kwargs):
    ti = kwargs['ti']
    valid_data = ti.xcom_pull(task_ids='data_quality_check', key='valid_data')
    dag_id = config['dag_id']
    
    current_time = datetime.now(timezone.utc)
    
    db = DBConnector()
    conn = db.get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("SELECT * FROM aircraft_states WHERE dag_id = %s", (dag_id,))
    existing_states_raw = cursor.fetchall()
    
    existing_states = {row['hex']: row for row in existing_states_raw}
    
    cursor.close()
    conn.close()
    
    evaluated_data = []
    email_needed = False
    
    current_hexes = {ac['hex'] for ac in valid_data}
    
    for aircraft in valid_data:
        ac_hex = aircraft['hex']
        
        if ac_hex not in existing_states:
            status = 'NEW_ENTRY'
            email_needed = True
        else:
            prev_state = existing_states[ac_hex]
            last_alert = prev_state['last_alert_time']
            
            if last_alert:
                if last_alert.tzinfo is None:
                    last_alert = last_alert.replace(tzinfo=timezone.utc)
                time_diff = (current_time - last_alert).total_seconds() / 60.0
                if time_diff >= 30:
                    status = 'RE_ENTRY_ALERT'
                    email_needed = True
                else:
                    status = 'INSIDE_NO_ALERT'
            else:
                status = 'NEW_ENTRY'
                email_needed = True
                
        aircraft['status'] = status
        aircraft['eval_time'] = current_time.isoformat()
        evaluated_data.append(aircraft)
        
    for ac_hex, state in existing_states.items():
        if ac_hex not in current_hexes and state['status'] != 'OUTSIDE':
            evaluated_data.append({
                'hex': ac_hex,
                'registration': state['registration'],
                'callsign': state['callsign'],
                'status': 'OUTSIDE',
                'eval_time': current_time.isoformat(),
                'lat': None,
                'lon': None
            })
            email_needed = True
            
    ti.xcom_push(key='evaluated_data', value=evaluated_data)
    ti.xcom_push(key='email_needed', value=email_needed)
