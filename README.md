# Третий (финальный) проект курса SkillFactory Data Analytics (SDA-6).

## Описание файлов

### Первая часть проекта, построение рекомендательной системы

- `final_proj_recommendations.ipynb` — Ноутбук Jupyter, для интерактивной работы. Создан из .py файла.
- `final_proj_recommendations.py` — Программа для выдачи таблицы рекомендованных курсов. Может быть загружена в iPython через `%load` и использоваться там в диалоговом режиме.
- `sample_recommended_pairs.csv` — Пример выдачи программы `final_proj_recommendations.py`

### Вторая часть проекта — планирование A/B теста и обработка его результатов

- `final_proj_abtest.ipynb` — Jupyter ноутбук с отчётом, расчётами и визуализацией.
- `final_proj_abtest.py` — Исходный файл для ноутбука, можно загрузить в iPython для интерактивной работы.

### Служебные файлы, компоненты и т.д.

- `img` — каталог с картинкой (структурой БД)
- `LocalPostgres.py` - модуль для работы с локальной БД в контейнере;
- `SkillFactory_DB.py` — модуль для работы с базой данных SkillFactory.
- `Pipfile` — файл для установки модулей Python, используется [pipenv](https://pipenv.pypa.io/en/latest/)
- `Pipfile.lock` - служебный файл pipenv
- `SConstruct` — задание для системы сборки [scons](https://scons.org)

## Требования к среде

Требуются пакеты:

- numpy
- sklearn
- statsmodels
- scons
- psycopg2
- matplotlib
- seaborn

Для интерактивной работы нужен Jupyter Notebook и/или iPython.  Подробности в файле `Pipfile`.
