from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner' : 'faiz',
    'retries': 5,
    'retry_delay' : timedelta( minutes=5)
    
}

@dag(dag_id='dag_task_api',
     default_args=default_args,
     start_date=datetime(2021,1,29),
     schedule_interval='@daily'
     )
def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return { "firstname": "ali",
                "lastname": "abu"}
    
    @task()
    def get_age():
        return 12
    
    @task()
    def greet(firstname,lastname, age):
        print(f"hello world my name is {firstname} { lastname}" f"and im {age} years old")
        
    name_dict = get_name()
    age = get_age()
    greet(firstname=name_dict['firstname'], lastname=name_dict['lastname'], age=age)
    
greet_dag = hello_world_etl()