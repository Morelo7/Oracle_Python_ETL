import oracledb
import pandas as pd
import os
from config import ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN

conn = oracledb.connect(
    user=ORACLE_USER,
    password=ORACLE_PASSWORD,
    dsn=ORACLE_DSN
)

cur = conn.cursor()

path_dep_base = cur.var(str)
data_dep = conn.cursor()

path_emp_file = cur.var(str)
data_emp = conn.cursor()

# call procedure
cur.callproc(
    "HR.GET_EMPLOYEES_DETAIL",
    [path_dep_base, data_dep, path_emp_file, data_emp]
)

# ---------- DEPARTMENT FILES ----------
rows_dep = data_dep.fetchall()
cols_dep = [col[0] for col in data_dep.description]

df_dep_all = pd.DataFrame(rows_dep, columns=cols_dep)

base_path = path_dep_base.getvalue()

os.makedirs(os.path.dirname(base_path), exist_ok=True)

for dep_name, df_dep in df_dep_all.groupby("DEPARTMENT_NAME"):
    file_path = f"{base_path}_{dep_name}.xlsx"
    df_dep.to_excel(file_path, index=False)

# ---------- EMPLOYEE FILE ----------
rows_emp = data_emp.fetchall()
cols_emp = [col[0] for col in data_emp.description]

df_emp = pd.DataFrame(rows_emp, columns=cols_emp)

emp_path = path_emp_file.getvalue()
df_emp.to_excel(emp_path, index=False)

# cleanup
data_dep.close()
data_emp.close()
cur.close()
conn.close()
