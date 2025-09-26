WITH emp AS (
  SELECT
    e.id            AS employee_id,
    e.name          AS employee_name,
    e.work_email,
    e.work_phone,
    e.mobile_phone,
    e.gender,
    e.birthday,
    e.active,
    e.company_id,
    e.department_id,
    e.job_id,
    e.create_date,
    e.write_date
  FROM hr_employee e
  WHERE e.write_date >= COALESCE(TIMESTAMP '{{ variables.last_emp_ts | default("1970-01-01") }}',
                                 TIMESTAMP '1970-01-01')
)
SELECT
  emp.*,
  d.name AS department_name,
  j.name AS job_name,
  c.name AS company_name
FROM emp
LEFT JOIN hr_department d ON d.id = emp.department_id
LEFT JOIN hr_job       j ON j.id = emp.job_id
LEFT JOIN res_company  c ON c.id = emp.company_id
ORDER BY emp.write_date DESC, emp.employee_id DESC;
