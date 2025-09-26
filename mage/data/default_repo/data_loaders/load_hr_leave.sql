SELECT
  l.id                 AS leave_id,
  l.employee_id,
  l.holiday_status_id  AS leave_type_id,
  l.request_date_from,
  l.request_date_to,
  l.number_of_days,
  l.state,
  l.create_date,
  l.write_date
FROM hr_leave l
WHERE l.write_date >= COALESCE(TIMESTAMP '{{ variables.last_leave_ts | default("1970-01-01") }}',
                               TIMESTAMP '1970-01-01')
ORDER BY l.write_date DESC, l.id DESC;
