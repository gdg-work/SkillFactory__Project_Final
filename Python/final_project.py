#!/usr/bin/env python
# —--
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
# —--

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
# ## Основные результаты

# Система имеет слабость: при появлении нового курса он не будет рекомендоваться, так как число его покупателей
# слишком мало.

# %% [markdown]
# ## Ход работы <a name='work_log'/>

# %% [markdown]
# Подключение модулей, определение констант

# %%
import pandas as pd
import numpy as np
import psycopg2
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import math

from psycopg2.extras import NamedTupleCursor
from itertools import combinations
from itertools import chain
from functools import partial
from collections import Counter
from LocalPostgres import DB_CONNECT_STRING
from matplotlib.axes._axes import _log as matplotlib_axes_logger

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
# В стандартном Python нет функции `display()`, эмулирую её через `print()`
# %%
try:
    __ = get_ipython()
    INTERACTIVE=1
except NameError:
    INTERACTIVE=0
    def display(*args, **kwargs):
        print(*args, **kwargs)

# %% [markdown]
# Инициализация графической подсистемы
#
# %%
sns.set()
matplotlib_axes_logger.setLevel('ERROR')  # disable color warnings in Matplotlib
sns.set_style('whitegrid')
plt.ioff()

# %% [markdown]
# ### Получение необходимых данных из базы


# %% [markdown]
# Определю некоторое количество констант, в которые сложу CTE.
# 
# Каждому пользователю ставится в соответствие купленный
# им курс. Если курсов больше одного, в выводе будет несколько
# записей с одним `user_id`

# %%
USER_COURSE_PAIRS = """\
user_course_pairs as (
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
)"""

# %% [markdown]
# Сколько клиентов покупали курсы?

# %%
BUYERS_COUNT="""\
buyers_count as (
    select count(distinct user_id) from user_course_pairs
)"""

# %% [markdown]
# ?: сколько всего есть разных курсов?
# Ответ "без понятия, но могу посмотреть количество разных купленных курсов за 2 года"

# %%
COURSES_IN_CARTS = """\
courses_count as (
    select count(distinct resource_id)
    from final.cart_items
        where resource_type = 'Course'
)"""

# %% [markdown]
# Количество купленных курсов (как оказалось, оно отличается от количества курсов в корзинах,
# какой-то один курс был положен в корзину, но никем не куплен)

# %%
COURSES_BOUGHT = """\
courses_bought as (
    select distinct course_id
    from user_course_pairs
)"""


# %% [markdown]
# Количество купленных курсов на пользователя  Выдаёт таблицу "ID пользователя - количество купленных курсов".

# %%
COURSES_CNT_BY_USER = """\
courses_count_by_user as (
    select user_id, count(course_id) as courses_count
    from user_course_pairs
    group by user_id
    order by user_id
)"""

# %% [markdown]
# Теперь нужно для каждого пользователя создать список курсов, которые он купил, но при условии, что таких курсов
# больше одного.  Разделяю список пробелами, так как это числа (т.е. внутри них пробелов быть не может),
# и по умолчанию `str.split()` разбивает строки по пробельным символам (white space).

# %%
COURSES_LIST_QUERY="""\
select
    distinct user_id,
    count(distinct course_id) as courses_cnt,
    STRING_AGG(distinct course_id::text, ' ') as courses_list
from user_course_pairs
group by user_id
having count(distinct course_id) > 1;
"""

# %% [markdown]
# Запускаю некоторые запросы, чтобы убедиться, что у меня CTE составлены правильно и выдаются ожидаемые
# результаты, которые я уже получил из базы вручную, в процессе отладки CTE.
#
# Запоминаю количество курсов, которые покупались, в переменной `bought_courses_cnt` (потом пригодится)
# и список идентификаторов курсов в списке bought_courseids_lst

# %%
cursor = init_connect(DB_CONNECT_STRING)
bought_courses_cnt =  psql_query(cursor, [USER_COURSE_PAIRS, COURSES_BOUGHT], 'select count(course_id) from courses_bought')[0][0]
bought_courseids_lst = psql_query(cursor, [USER_COURSE_PAIRS, COURSES_BOUGHT], 'select course_id from courses_bought')

assert(psql_query(cursor, [USER_COURSE_PAIRS, BUYERS_COUNT], "select * from buyers_count")[0][0] == 49006)
assert(psql_query(cursor, [COURSES_IN_CARTS], 'select * from courses_count')[0][0] == 127)
assert(bought_courses_cnt == 126)
assert(len(bought_courseids_lst) == 126)

# %% 
# now we have N touples in list bought_courseids_lst, and we need a numpy array there.
# So, peel off the tuple with list comprehension and make an array from resulting list
course_ids_a = np.array([id for (id,) in bought_courseids_lst])
if INTERACTIVE: print(course_ids_a) 

#
# Теперь обрабатываем полученный список пользователей.
# Думаю, что для каждого пользователя надо создать set frozenset-ов, где на 2-м уровне будут все пары курсов из тех,
# которые он приобретал.  Тогда можно посмотреть, какие пары самые популярные.  А общий набор легко получить объединением
# всех сетов, при этом дубликаты автоматически исчезнут.  Или с использованием Counter по frozenset-ам.
#
# Интересный случай, когда пользователь покупал одни и те же курсы больше одного раза.  Ситуацию, когда
# пользователь купил только два одинаковых курса, обрезаем в SQL (`having count(distinct course_id) > 1`),
# но сюда могут прокрасться заказчики, купившие больше 2 курсов, причём есть хотя бы одна пара одинаковых.
# Избавляемся от таких преобразованием во множество (`set`)
# UPD: избавился и от таких случаев в SQL, вставив 'distinct' в группировку, но преобразование в множество
# оставил.

# %%
user_count=0
users_data_d = {}  # {uid: { {course, course}, {course, course}, ... } }
all_courses = set()
pairs_count = Counter()
ids_count = Counter()

for (users_count, (user_id, courses_cnt, user_courses_str)) in enumerate(large_query(cursor, [USER_COURSE_PAIRS], COURSES_LIST_QUERY)):
    s_courses = set( [int(i) for i in user_courses_str.split()] )  # default split on whitespace
    assert(len(s_courses) == courses_cnt)
    # increase counters for individual courses
    for course_id in s_courses:
        ids_count[course_id] += 1
    l_pairs = combinations(s_courses, 2)
    users_data_d[user_id] = set([frozenset(t) for t in l_pairs])
    for pair in users_data_d[user_id]:
        pairs_count[pair] += 1
else:
    if INTERACTIVE: print(f"Total users: {users_count}")

assert(len(ids_count) == 126)

def print_tops(ids_count, pairs_count):
    """Печатает верхние и нижние N курсов и пар. Параметры: 1) счётчик курсов, 2) счётчик пар."""
    TOP_COUNT=5
    print(f"Верхние {TOP_COUNT} самых покупаемых курсов и их пар")
    print(ids_count.most_common(TOP_COUNT))
    print(pairs_count.most_common(TOP_COUNT))
    print(f"Нижние {TOP_COUNT} самых непопулярных курсов и пар")
    print(ids_count.most_common()[-TOP_COUNT:-1])
    print(pairs_count.most_common()[-TOP_COUNT:-1])
    return

if INTERACTIVE:
    print_tops(ids_count, pairs_count)

# %% [markdown]
# Построю график количества заказов на курсы, начиная с самых популярных. Масштаб по оси Y логарифмический.

# %%
if INTERACTIVE:
    pop_courses = pd.Series({id: cnt for id, cnt in ids_count.most_common()}) 
    fig = plt.figure(figsize=(12,7)) 
    ax = pop_courses.plot(kind='bar') 
    plt.yscale('log')
    plt.xticks([])
    plt.show()

# %% [markdown]
# Логарифмический масштаб помог выявить на графике три группы курсов:
# 1. Два-три самых популярных (551, 566 и, возможно, 515).
# 2. Основная часть курсов находится на экспоненте, которая спускается от тысячи заказов примерно до 15-ти.
# 3. Малопопулярные курсы, каждый из которых нашёл себе менее 15 покупателей. Таких немного, около двадцати,
# но с ними нужно что-то решать: или это курсы, которые только начали предлагатся, и нужно их рекламировать,
# или это курсы, популярность которых мала по другим причинам —- и нужно разбираться, почему их не покупают.


# %% [markdown]
# Занятно, что в списке возможных комбинаций мы получаем
# [полный граф](https://ru.wikipedia.org/wiki/%D0%9F%D0%BE%D0%BB%D0%BD%D1%8B%D0%B9_%D0%B3%D1%80%D0%B0%D1%84)
# из N вершин, где N - количество купленных заказчиком курсов.

# %% [markdown]
# Общее количество найденных пар курсов: 3989 (это длина счётчика пар), это означает, что матрица сочетаний курсов
# получается довольно «рыхлой»: всего возможно 126×126, то есть 15876 сочетаний, наблюдается примерно четверть.
# Самая популярная пара 566+551.

# %% [markdown]
# ### Построение таблицы для выдачи рекомендаций
# ### Построение матрицы сочетаний курсов

# %% [markdown]
# Интересный момент -- что делать, если в пару рекомендуемых курсов попадает курс их «хвоста» распределения, т. е.
# малопопулярный.  Возможно, есть смысл предлагать в этом случае самые популярные курсы?  Тут бы была полезна
# модификация исходных данных —- система тегов, чтобы не предлагать в пару к курсу по Python курс по разведению
# роз.  Будем рабоать с тем, что есть.  За малопопулярный курс примем такой, у которого число покупателей попадает
# в последние 5% (квантиль 5%).

# %%
# Чтобы номера курсов были в индексе, а количество в значении, пары нужно перевести в словарь
freq_table = pd.Series({k:v for k,v in ids_count.most_common()})

# %% [markdown]
# Нам понадобится какая-то точка отсчёта для «непопулярных» или, другими словами, редко встречающихся
# курсов. В качестве такой точки возьму 10% квантиль популярности курса.

# %%
unpopular_threshold = math.ceil(freq_table.quantile(0.1))

# %% [markdown]
# Функция для построения двумерной матрицы из таблицы частоты встречаемости пар курсов

# %%
def make_freq_matrix(pairs_count_dict: "Dictionary with pair as a key and count as a value") -> pd.DataFrame:
    """
    Построение двумерной матрицы сочетаний курсов. Параметры:
        1) Словарь, где каждому  ID курса соответствует Counter вида {(пара_курсов): число_встреченных}
    """
    pairs_df = pd.DataFrame(index=freq_table.index, columns=freq_table.index, dtype=np.uint32, data=0)
    for course_pair in pairs_count_dict.keys():
        (course_1, course_2) = course_pair
        pairs_df.loc[course_1, course_2] = pairs_count_dict[course_pair]
        pairs_df.loc[course_2, course_1] = pairs_count_dict[course_pair]
    return pairs_df

# %% [markdown]
# Имеем: таблицу pairs_count, где каждой паре поставлена в соответсвие её частота встречаемости.
# Строю: таблицу, где ID курсов по вертикали и горизонтали (их не так много). Делаю по обеим осям
# сортировку по частоте курсов: логично предположить, что те курсы, которые покупаются чаще всего,
# будут образовывать и наиболее часто встрачающиеся пары.

# %%
course_pairs_df = make_freq_matrix(pairs_count)

# %% [markdown]
# Таблица 126×126 слишком велика для удобного отображения на экране, но если преобразовать её в тепловую
# карту, её можно воспринимать.  Карта, конечно, будет симметричной относительно диагонали, которая идёт
# от "частого" угла (левого верхнего) в "редкий" (правый нижний).

# %%
if INTERACTIVE:
    plt.figure(figsize=(12,12))
    plt.title('Популярность пар курсов. По осям популярность уменьшается слева направо и сверху вниз')
    g = sns.heatmap(course_pairs_df, annot=False, linewidths=0.0, cmap=sns.color_palette('OrRd', 100))
    g.set_xticks([])
    g.set_yticks([])
    plt.show()

# %% [markdown]
# Популярность пар курсов оказалась крайне неравномерной, и подтверждается предположение о том, что самые
# популярные пары, в общем, составлены из самых популярных курсов.
#
# Посмотрим в цифрах «частый» (левый верхний) угол и «редкий» (правый нижний) углы матрицы

# %%
if INTERACTIVE:
    TOP_ANGLE_COUNT=20
    display(course_pairs_df.iloc[0:TOP_ANGLE_COUNT,0:TOP_ANGLE_COUNT])
    MINUS_TOP = (-1) * TOP_ANGLE_COUNT - 1
    display(course_pairs_df.iloc[MINUS_TOP:-1,MINUS_TOP:-1])

# %% [markdown]
# Как и следовало ожидать, самые частые сочетания из часто покупаемых курсов, редко покупаемые
# курсы и пары образуют нечасто.

# %% [markdown]
# ### Построение таблицы рекомендаций
 
# %% [markdown]
# Имея матрицу с частотой пар, нетрудно построить требуемую таблицу. Напоминаю требования к
# колонкам этой таблицы
#
# 1. курс, __к которому__ выдаются рекомендации
# 2. курс, который встречается в паре с курсом из первой колонки чаще всего
# 3. второй по встречаемости в паре с курсом из первой колонки курс.
#
# Если частота хотя бы одного из курсов пары меньше критической, этот курс
# заменяется в рекомендации на самый популярный.  Не знаю, насколько такая замена
# оправдана, на мой взгляд, система тегов помогла бы выдавать более осмысленные
# рекомендации.

# %%
def get_recommended_courses(pairs_df, threshold, freq_courses) -> pd.DataFrame:
    """ Функция строит массив рекомендаций по данным, порог и таблице частоты пар курсов.
    Параметры:
        1) DataFrame пар курсов, где идентификаторы по вертикали и горизонтали, количество пар на пересечениях.
        2) пороговое значение для признания курса малопопулярным.
        3) Series, в которой собраны курсы по порядку убывания популярности.
    Возвращает:
        pd.DataFrame с ID курса в индексе, рекомендациями в колонках 'first_rec' и 'second_rec'.
    """
    res = pd.DataFrame(index=pairs_df.index, dtype=np.uint32)
    for (row_idx, courses_paired) in pairs_df.iterrows(): 
        max_two = courses_paired.sort_values(ascending=False)[0:2]
        # И тут вспоминаю, что есть ограничение "курс слишком непопулярный".  У нас max_two.iloc[0] >= max_two.iloc[1]
        if max_two.iloc[1] > threshold:
            res.loc[row_idx, 'first_rec']  = max_two.index[0]
            res.loc[row_idx, 'second_rec'] = max_two.index[1]
            res.loc[row_idx, 'rec1_cnt']   = max_two.iloc[0]
            res.loc[row_idx, 'rec2_cnt']   = max_two.iloc[1]
        elif max_two.iloc[0] > threshold:
            res.loc[row_idx, 'first_rec']  = max_two.index[0]
            res.loc[row_idx, 'rec1_cnt']   = max_two.iloc[0]
            res.loc[row_idx, 'second_rec'] = freq_courses.index[0]
            res.loc[row_idx, 'rec2_cnt']   = freq_courses.iloc[0]
        else:
            res.loc[row_idx, 'first_rec']  = freq_courses.index[0]
            res.loc[row_idx, 'second_rec'] = freq_courses.index[1]
            res.loc[row_idx, 'rec1_cnt']   = freq_courses.iloc[0]
            res.loc[row_idx, 'rec2_cnt']   = freq_courses.iloc[1]
    res = res.drop(['rec1_cnt', 'rec2_cnt'], axis=1).astype(np.uint32)
    return res

res = get_recommended_courses(course_pairs_df, unpopular_threshold, freq_table)

# %% [markdown]
# В дата-фрейме 'res' содержатся рекомендуемые курсы. Структура датафрейма: индекс - курс,
# к которому даётся рекомендация, первая колонка (first_rec) -- первая рекомендация, вторая
# колонка -- вторая рекомендация.  Например, для получения обоих рекомендаций к курсу 489 нужно
# вызвать: `res.loc[489]`, для получения только первой -- `res.loc[489, 'first_rec'], для второй --
# `res.loc[289, 'second_rec']`.

# %%
if INTERACTIVE:
    display(res.loc[489], res.loc[489, 'first_rec'], res.loc[489, 'second_rec'])

# %% [markdown]
# Легко вывести эту таблицу в CSV или Excel.  Более универсален вывод в CSV, при вызове этого файла
# как программы -- выдаётся именно этот формат на стандартный вывод.

if __name__ == "__main__" and not INTERACTIVE:
    # called as a standalone program, so print recommendations list and exit
    print(get_recommended_courses(course_pairs_df, unpopular_threshold, freq_table).to_csv())
