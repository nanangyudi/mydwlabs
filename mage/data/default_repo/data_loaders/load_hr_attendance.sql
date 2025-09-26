SELECT
  a.id           AS attendance_id,
  a.employee_id,
  a.check_in,
  a.check_out,
  a.worked_hours,
  a.create_date,
  a.write_date
FROM hr_attendance a
WHERE a.write_date >= COALESCE(TIMESTAMP '{{ variables.last_att_ts | default("1970-01-01") }}',
                               TIMESTAMP '1970-01-01')
ORDER BY a.write_date DESC, a.id DESC;
