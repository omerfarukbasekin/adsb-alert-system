def data_quality_check(config, **kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data', key='raw_data')
    
    aircraft_list = raw_data.get('ac', [])
    valid_records = []
    
    import logging
    logger = logging.getLogger(__name__)

    target_categories = config.get('target_filters', {}).get('category_codes', [])
    target_keywords = config.get('target_filters', {}).get('description_keywords', [])
    
    logger.info(f"Total AC pulled: {len(aircraft_list)}. Filter lists: {target_categories}, {target_keywords}")
    
    for aircraft in aircraft_list:
        if not aircraft.get('hex'):
            continue
        
        if 'lat' not in aircraft or 'lon' not in aircraft:
            continue
            
        try:
            float(aircraft['lat'])
            float(aircraft['lon'])
        except (ValueError, TypeError):
            continue

        category_match = False
        if target_categories:
            category = str(aircraft.get('category', ''))
            if category in target_categories:
                category_match = True

        keyword_match = False
        if target_keywords:
            desc = str(aircraft.get('desc', '')).upper()
            type_desc = str(aircraft.get('t', '')).upper()
            combined_text = f"{desc} {type_desc}"
            
            import re
            for kw in target_keywords:
                kw_upper = kw.upper()
                if re.search(r'\b' + re.escape(kw_upper) + r'\b', combined_text):
                    keyword_match = True
                    break
            
        if not target_categories and not target_keywords:
            is_valid = True
        else:
            is_valid = category_match or keyword_match
            
        if is_valid:
            logger.info(f"MATCH FOUND -> Hex: {aircraft['hex']}, Type: {aircraft.get('t')}, Desc: {aircraft.get('desc')}")
            valid_records.append({
                'hex': aircraft['hex'],
                'registration': aircraft.get('r', ''),
                'callsign': aircraft.get('flight', '').strip(),
                'lat': aircraft['lat'],
                'lon': aircraft['lon']
            })
            
    ti.xcom_push(key='valid_data', value=valid_records)
