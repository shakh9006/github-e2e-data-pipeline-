def get_dates(**context):
    start_date = context['data_interval_start']
    end_date = context['data_interval_end']
    
    return start_date, end_date