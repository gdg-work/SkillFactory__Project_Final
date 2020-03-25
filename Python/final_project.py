#!/usr/bin/env python
# -*- coding: utf-8 -*-
# —-
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# —-

# %% [markdown]
# # Рекомендательная система для курсов SkillFactory
#
# ### Содержание
#
# * [Постановка задачи](#tasks)
#   - 1. [Выдача рекомендаций по ID проданного курса](#adv_additional_courses)
#   - 2. Обработка результатов сплит-теста рекомендаций 
# * Основные результаты
#   -
#   
# * Ход работы
# * [Знакомство с данными](#describe_data)
# 

# %% [markdown]
# ## Постановка задачи <a name='tasks'/>
# 
# %% [markdown]
# ### Выдача рекомендаций по ID проданного курса <a name='adv_additional_courses'/>
#
# %% [markdown]
# Нужна рекомендательная система по курсам. Cоставить для продакт-менджеров таблицу,
# где каждому ID курса будут предоставлены ещё два курса, которые будут рекомендаваться
#
# Уточнение:
# 
# Оформить таблицу с рекомендациями для продакт-менеджера и отдела маркетинга.
# 
# Составьте таблицу с тремя столбцами:
# 
#   1. Курс, к которому идёт рекомендация
#   2. Курс для рекомендации № 1 (самый популярный)
#   3. Курс для рекомендации № 2 (второй по популярности).
# 
# А что делать, если одна из рекомендаций встречается слишком мало раз? В таком случае:
#  - нужно установить минимальную границу — какое количество считать слишком
#    малым.
#  - вместо такого малопопулярного курса выводите какой-то другой курс, который, на
#    ваш аргументированный взгляд, подходит лучше.


# %% [markdown]
# ## Ход работы <a name='work_log'/>

# %% [markdown]
# Подключение модулей, определение констант

# %%
import pandas as pd
import psycopg2
from psycopg2.extras import NamedTupleCursor
from itertools import combinations
# import matplotlib as mpl
# import matplotlib.pyplot as plt

from LocalPostgres import DB_CONNECT_STRING
from itertools import chain
from functools import partial

# %% [markdown]
# ## Служебные функции
#
# %% [markdown]
# Подключение к базе, возвращает курсор

# %%
def init_connect(conn_string) -> psycopg2.extensions.cursor:
    db_conn = psycopg2.connect(conn_string, cursor_factory=NamedTupleCursor)
    cursor = None
    if db_conn:
        db_conn.set_session(readonly=True,autocommit=True)
        cursor = db_conn.cursor()
    return cursor

# %% [markdown]
# Служебная функция, помогает отформатировать запросы в базу

# %%
def _format_select(ctes: "list of CTEs", select: "SQL select") -> str:
    """
    Форматирует SQL запрос из списка CTE и выражения 'select',
    заданных соответственно первым и вторым параметрами
    Возвращает запрос в виде строки.
    """
    query = ( "with " + ", ".join(ctes) + " " + select)
    if query[-1] != ';':
        query = query + ';'
    return query

# %% [markdown]
# Запрос в базу

# %%
def psql_query(cursor: "Database cursor", ctes: "list of CTEs", 
               select: "select sql query as a string") -> list:
    """
    Запрос, возвращающий из базы много данных.
    Параметры: 1) Курсор PsycoPg2, 3) запрос SQL в виде строки.
    Возвращает: список Named Tuple-ов -- результат запроса. Если результатов
    нет, будет возвращён None.
    """
    query = _format_select(ctes, select)
    # print("*DBG* multiline_query will execute:\n" + query)
    cursor.execute(query)
    return cursor.fetchall()

# %%
# Решение со [StackOverflow](https://stackoverflow.com/a/57015290) для
# выполнения большого запроса в базу так, чтобы не сожрать всю память.
# SA: https://docs.python.org/3/library/itertools.html#itertools.chain,\
#     https://docs.python.org/3.5/library/functools.html#functools.partial
def large_query(cursor: "DB Cursor", ctes: "list of CTEs", sql: "SQL query",
				chunksize=1000) -> chain:

    query = _format_select(ctes, sql)
    cursor.execute(query)
    chunks_until_empty = iter(partial(cursor.fetchmany, chunksize), [])
    return chain.from_iterable(chunks_until_empty)

# %% [markdown]
# ### Получение необходимых данных из базы


# %% [markdown]
# Определю некоторое количество констант, в которые сложу CTE.
# 
# Каждому пользователю ставится в соответствие купленный
# им курс. Если курсов больше одного, в выводе будет несколько
# записей с одним `user_id`

# %%
USER_COURSE_PAIRS = """user_course_pairs as (
    select user_id, resource_id as course_id
    from
    	final.carts as c
    	join final.cart_items as i
    	on c.id = i.cart_id
    where
    	i.resource_type = 'Course'
        and
        c.state = 'successful'
    order by user_id
    )
"""

# %% [markdown]
# Сколько клиентов покупали курсы?

# %%
BUYERS_COUNT="""buyers_count as (
    select count(distinct user_id) from user_course_pairs
)"""

# %% [markdown]
# ?: сколько всего есть разных курсов?
# Ответ "без понятия, но могу посмотреть количество разных купленных курсов за 2 года"

# %%
COURSES_IN_CARTS = """courses_count as (
    select count(distinct resource_id)
    from final.cart_items
	where resource_type = 'Course'
)"""

# %% [markdown]
# Количество купленных курсов (как оказалось, оно отличается от количества курсов в корзинах,
# какой-то один курс был положен в корзину, но никем не куплен)

# %%
COURSES_BOUGHT = """courses_bought as (
    select count(distinct course_id)
    from user_course_pairs
)"""


# %% [markdown]
# Количество купленных курсов на пользователя  Выдаёт таблицу "ID пользователя - количество купленных курсов".

# %%
COURSES_CNT_BY_USER = """courses_count_by_user as (
    select user_id, count(course_id) as courses_count
    from user_course_pairs
    group by user_id
    order by user_id
)"""

# %% [markdown]
# Запускаю некоторые запросы, чтобы убедиться, что у меня CTE составлены правильно, и выдаются ожидаемые
# результаты, которые я уже получил из базы вручную, в процессе отладки CTE.

# %%
cursor = init_connect(DB_CONNECT_STRING)
assert(psql_query(cursor, [USER_COURSE_PAIRS, BUYERS_COUNT], "select * from buyers_count")[0][0] == 49006)
assert(psql_query(cursor, [COURSES_IN_CARTS], 'select * from courses_count')[0][0] == 127)
assert(psql_query(cursor, [USER_COURSE_PAIRS, COURSES_BOUGHT], 'select * from courses_bought')[0][0] == 126)



# %% [markdown]
# Теперь нужно для каждого пользователя создать список курсов, которые он купил, но при условии, что таких курсов
# больше одного.

# %%
COURSES_LIST_QUERY="""select
	distinct user_id,
	count(distinct course_id) as courses_cnt,
    STRING_AGG(course_id::text, ' ') as courses_list
from user_course_pairs
group by user_id
having count(distinct course_id) > 1;
"""
# l = psql_query(cursor, [USER_COURSE_PAIRS], COURSES_LIST_QUERY)
# print(len(l))


#
# Теперь обрабатываем полученный список пользователей.
# Думаю, что для каждого пользователя надо созать set frozenset-ов, где на 2-м уровне будут все пары курсов из тех,
# которые он приобретал.  Тогда можно посмотреть, какие пары самые популярные.  А общий набор легко получить объединением
# всех сетов, при этом дубликаты автоматически кончатся.  Или с использованием Counter по frozenset-ам.
user_count=0
for user_data in large_query(cursor, [USER_COURSE_PAIRS], COURSES_LIST_QUERY):
    user_count += 1
print(user_count)
