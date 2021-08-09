# psycopg2 is the Python-PostgreSQL Database Adapter
import psycopg2

# pandas library is used for manipulation and analysis
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO)

# Function for getting the Emp Num,Name and Manager Name and saving it to Solu.xlsx
def solution1(cur):
    # for getting the Emp Num,Name and Manager Num and using inner join to replace the manager number with the name
    cur.execute("select e.empno,e.ename,m.ename from emp as e inner join emp as m on e.mgr=m.empno")

    # For fetching all the rows
    df=pd.DataFrame(cur.fetchall())

    # For exporting the dataframe to the Solu.xlsx file
    df.to_excel('Solu.xlsx',header=["E_Id","E_Name","M_Name"],index=False)

# Function for getting the total compensation given and storing it in the xlxs file
def solution2(cur):
    cur.execute(
        "select e.empno,e.ename,d.dname,comms.comm*comms.mon,comms.mon "
        "from (select empno,Cast(((Cast (Current_date-min(startdate) as float))/365.0*12) as Int) as mon,"
        ""
        "sum(CASE when comm is not null then comm else 0 end) comm from jobhist group by(empno)) comms "
        "inner join emp e on e.empno=comms.empno inner join dept d on d.deptno=e.deptno")
    df2 = pd.DataFrame(cur.fetchall())
    
    # command for saving output to compensation.xlsx
    df2.to_excel('compensation.xlsx',
                 header=["Emm No", "Emp Name", "Dept Name", "Total Compensation", "Months Spent in Organization"],
                 index=False)
    #command for saving the output in csv format for further processing
    df2.to_csv('c.csv',
               header=["Emp No", "Emp Name", "Dept Name", "Total Compensation", "Months Spent in Organization"],
               index=False)

# function for taking the input through csv file and 
def solution3(cur):

    cur.execute("COPY employee FROM '/Users/sachin/Desktop/c.csv' "
                "DELIMITER ',' CSV HEADER;")


def solution4(cur):

    cur.execute(
        "select d.deptno,d.dname,Case when e.c is null then 0 else e.c end from "
        "(select depname,sum(compensation)as c from employee group by(depname)) e right join dept d on d.dname=e.depname")
    df3 = pd.DataFrame(cur.fetchall())
    df3.to_excel('deptcomm.xlsx', header=["Dept No", "Dept Name", "Compensation"], index=False)



def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "localhost",
                                  port = "5432",
                                  database = "Employees")

        # create a cursor
        cur = conn.cursor()

        # close the communication with the PostgreSQL

        solution1(cur)

        solution2(cur)

        solution3(cur)

        solution4(cur)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
