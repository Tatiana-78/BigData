#!/usr/bin/env python3
import sys
import io
import json

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
output_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
stderr_stream = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

for line in input_stream:
    data = json.loads(line.strip()) 
  
    sum_minutes = 0

    if data.get('is_open') == 0:
        week_minutes=0
        print(f'{data["business_id"]}\t{int(week_minutes)}', file=output_stream)

    elif data.get('is_open') == 1:
        week_minutes = 0
        hours = data.get('hours', None)

        if hours is not None:
            for day, time_range in hours.items():
                start_time, finish_time = time_range.split('-')
                start_hour, start_minute = map(int, start_time.split(':'))
                finish_hour, finish_minute = map(int, finish_time.split(':'))

                # Если бизнес закрывается после полуночи
                if finish_hour < start_hour or (finish_hour == start_hour and finish_minute < start_minute):
                    finish_hour += 24

                day_minutes = (finish_hour - start_hour)*60 + (finish_minute - start_minute)
                week_minutes += day_minutes
         
        print(f'{data["business_id"]}\t{week_minutes}', file
