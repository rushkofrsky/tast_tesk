import datetime
import os
import shutil
import virtualenv
import git
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Шаг 1: Импорты # Шаг 2: Определение функции 'execute_task'
def execute_task(git_url, module_name, function_name, arguments):
    # Шаг 3: Выполнение задачи
    try:
        # 3.1: Клонирование Git-репозитория в указанную директорию
        repo_dir = '/path/to/repo'  # Замените на актуальный путь
        git.Repo.clone_from(git_url, repo_dir)

        # 3.2: Создание виртуального окружения
        venv_dir = '/path/to/venv'  # Актуализируйте
        virtualenv.create_environment(venv_dir)

        # 3.3: Активация виртуального окружения и установка зависимостей из requirements.txt
        activate_script = os.path.join(venv_dir, 'bin', 'activate')  # Для Linux/Mac
        # activate_script = os.path.join(venv_dir, 'Scripts', 'activate')  # Если Венда
        os.system(f"source {activate_script} && pip install -r {repo_dir}/requirements.txt")

        # 3.4: Импорт модуля и вызов функции с аргументами
        sys.path.append(repo_dir)
        module = __import__(module_name)
        function = getattr(module, function_name)
        result = function(*arguments)

        # 3.5: Подчищевание после выполнения
        os.system(f"deactivate")  # Деактивация виртуального окружения
        shutil.rmtree(venv_dir)  # Удаление виртуального окружения
        shutil.rmtree(repo_dir)  # Удаление склонированного репозитория

        return result

  # Шаг 4: Обработчик ошибок
    except Exception as e:
        return str(e)

# Шаг 5: Создание объекта DAG
dag = DAG(
    'git_task_dag',
    schedule_interval='@daily',  # Расписание выполнения
    start_date=datetime.datetime(2023, 10, 26),  # Дата начала выполнения
)

# Шаг 6: Определение оператора 'git_task'
git_task_operator = PythonOperator(
    task_id='git_task',
    python_callable=execute_task,
    op_args=['git_repo_url', 'module_name', 'function_name', [arg1, arg2, arg3]],  # Передаваемые аргументы
    dag=dag
)

# Шаг 7: Определение зависимостей (если необходимо)
# git_task_operator.set_upstream(...)
# git_task_operator.set_downstream(...)

# Шаг 8: Запуск DAG
if __name__ == '__main__':
    dag.cli()
