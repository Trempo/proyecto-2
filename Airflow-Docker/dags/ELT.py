# Utilidades de airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime, timedelta
from utils.file_util import *

# Funciones ETL
from utils.crear_tablas import crear_tablas
from utils.insert_queries import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL",
    description='ETL',
    schedule_interval='@monthly', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="postgres_localhost", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas()
    )
 
    descargar_datos = PythonOperator(
        task_id = "descargar_datos",
        python_callable = descargar_datos,
        dag = dag
    )

    crear_archivos = PythonOperator(
        task_id = "crear_archivos",
        python_callable = crear_archivos,
        dag = dag
    )

    # task: 2 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 2.1 poblar tabla city
        poblar_date = PostgresOperator(
            task_id="poblar_date_table",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_date(csv_path = "dimension_date_table")
        )

    
   
        # task: 2.2 poblar tabla customer
        poblar_departamento = PostgresOperator(
            task_id="poblar_departamento",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_departamento(csv_path ="dimension_departamento")
        )

            # task: 2.3 poblar tabla date
        poblar_entidad = PostgresOperator(
            task_id="poblar_entidad",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_entidad(csv_path = "dimension_entidad")
        )



    # task: 3 poblar la tabla de hechos
    poblar_fact_indicador = PostgresOperator(
            task_id="construir_tabla_de_hechos",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_fact_indicador(csv_path = "fact_indicador")
    )


    
    # flujo de ejecución de las tareas  
    crear_tablas_db >> descargar_datos >> crear_archivos >> poblar_tablas_dimensiones >> poblar_fact_indicador
    
