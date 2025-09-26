-- apakah tabel HR ada?
--SELECT tablename
--FROM pg_catalog.pg_tables
--WHERE schemaname = 'public'
--AND tablename IN ('hr_employee','hr_attendance','hr_leave');

-- hitung baris (akan error kalau modul belum terpasang)
--SELECT count(*) FROM hr_employee;
--SELECT count(*) FROM hr_attendance;
SELECT count(*) FROM hr_leave;
