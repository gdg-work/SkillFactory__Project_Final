#!/usr/bin/env python
# %% [markdown]
# # Рекомендательная система для курсов SkillFactory
#
# ### Содержание <a name='contents'/>
# 
# * [Основные результаты](#results)
#   - [Принцип генерации таблицы](#res.principle)
#   - [Принятые компромиссы и ограничения](#res.limitations)
#   - [Дальнейшее развитие системы](#res.development)
# * [Постановка задачи](#tasks)
#   - [Выдача рекомендаций по ID проданного курса](#adv_additional_courses)
#   - [Описание данных](#describe_data)
# * [Программа, расчёты и визуализации](#program)
#   - [Определения констант и функций](#const)
#     + [Функции для работы с базой данных](#db_functions)
#     + [Функции, связанные с интерактивной работой в Jupyter Notebook](#nb_functions)
#     + [Константы для создания SQL запросов](#const_cte)
#     + [Функции для получения конкретных данных из базы](#data_gather)
#     + [Построение матрицы сочетаний курсов](#matrix_create)
#     + [Построение таблицы рекомендаций](#recommendations)
#     + [Функция для интерактивной работы (Jupyter notebook или iPython)](#interactive)
#     + [Функция для пакетной работы](#batch)
#   - [Основная точка входа в программу](#exec_point)
#   - [Распечатка выходных данных и визуализации](#output)

# %% [markdown]
# ## Основные результаты <a name='results'/>
# %% [markdown]
# Разработана программа, которая по идентификатору (ID) курса предлагает ещё два
# ID курсов для допродажи.  Программа выбирает эти  курсы на основании частотной
# таблицы пар курсов.
#
# Формат выдачи: ID курса, рекомендуемый ID 1, рекомендуемый ID2.
#
# | ID курса  | Рекомендация 1 | Рекомендация 2 |
# |:----------|---------------:|---------------:|
# | 356 | 571 | 357 |
# | 357 | 571 | 356 |
# | 358 | 570 | 752 |
# | ... | ... | ... |
#
# Сортировка по первому полю (идентификатор курса)
#
# При запуске программа `final_proj_recommendations.py` запрашивает текущие данные о продажах
# курсов из БД, строит по ним таблицу рекомендаций и печатает эту таблицу на стандартный
# вывод в CSV формате.  Возможна доработка программы для выдачи других форматов или
# работы в составе API.
#
# %% [markdown]
# ### Принцип генерации таблицы <a name='res.principle'/>
# %% [markdown]
# На основании истории продаж к каждому идентификатору (ID) курса рекомендуется ещё два
# ID$_n$ курсов, которые чаще всего продавались вместе с ним.  «Вместе с ним» означает «проданы
# тому же пользователю», но не обязательно в одной корзине.

# %% [markdown]
# ### Принятые компромиссы и ограничения <a name='res.limitations'/>
# %% [markdown]
# 1. Если среди кандидатов на рекомендацию в частотной таблице нет популярных курсов,
# то рекомендуются самые продаваемые курсы из базы данных.
# Таким образом, в рекомендациях к редко покупаемым курсам окажутся лидеры продаж.
# 
# 2. «Непопулярные» курсы — это курсы из нижних 5% общего списка
# курсов, отсортированного по убыванию частоты продажи.
#
# 3. Новые курсы, которые клиенты ещё не купили достаточное число раз, не будут показываться
# в рекомендациях, таким образом, рекомендательная система может _затруднить_ продажи новых курсов.

# %% [markdown]
# ### Дальнейшее развитие системы <a name='res.development'/>
# %% [markdown]
# 1. Рекомендую внедрить систему тегов для курсов. Например, курс SDA-6 может получить теги «аналитика»,
# «sql», «python», «statistics», «data engineering», «data science», «specialist», «6_mon». В этом
# случае можно построить более точную систему рекомендаций.
#
# 2. Было бы полезно внедрить систему оценок курсов студентами, тогда появится возможность выбирать пары
# не только из совместно проданных курсов, но и из наиболее высоко оцененных.
#
# 3. Модифицировать программу так, чтобы она не рекомендовала пользователю те курсы,
# которые он уже купил. Но в таком случае программа должна работать в режиме API, потому что потребуются
# дополнительные запросы в базу.
#
# Дальнейшие разделы описывают данные и ход работы.

# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#results)  |
# |:--------------------------|-------------------------------:|

# %% [markdown]
# ## Постановка задачи <a name='tasks'/>
# %% [markdown]
# ### Выдача рекомендаций по ID проданного курса <a name='adv_additional_courses'/>

# %% [markdown]
# У продакт-менеджера есть идея организовать допродажу в корзине для увеличения среднего чека
# Для этого нужна рекомендательная система по курсам. Cоставить для продакт-менеджеров таблицу,
# где к каждому ID проданного курса будут предложены ещё два ID рекомендуемых курсов.

# #### Уточнение:
# 
# Оформить таблицу с рекомендациями для продакт-менеджера и отдела маркетинга.
# 
# Составьте таблицу с тремя столбцами:
# 
#   1. Курс, к которому идёт рекомендация
#   2. Рекомендованный курс № 1 (чаще всего покупаемый вместе с курсом из п. 1)
#   3. Рекомендованный курс № 2 (второй по популярности среди покупаемых с курсом из п. 1).
# 
# А что делать, если одна из рекомендаций встречается слишком мало раз? В таком случае:
#  - нужно установить минимальную границу — какое количество считать слишком малым.
#  - вместо такого малопопулярного курса выводите какой-то другой курс, который, на
#    ваш аргументированный взгляд, подходит лучше.
#    
# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#tasks)  |
# |:--------------------------|-------------------------------:|

# %% [markdown]
# ### Описание данных <a name='describe_data'/>
# %% [markdown]
# Исходные данные находятся в БД, в схеме `final`. Это две таблицы: `carts` и `carts_items`.
# 
# <img src='img/tables.svg' alt="Структура таблиц схемы final" width="70%"/>
#  
# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#tasks)  |
# |:---|---:|

# %% [markdown]
# ## Программа, расчёты и визуализации <a name='program'/>
# %% [markdown]
# Подключение модулей, определение констант

# %%
import pandas as pd
import numpy as np
import psycopg2
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from psycopg2.extras import NamedTupleCursor
from itertools import combinations
from itertools import chain
from functools import partial
from collections import Counter
# -- on my local machine, I keep passwords in separate files
# from LocalPostgres import DB_CONNECT_STRING
from SkillFactory_DB import DB_CONNECT_STRING
from matplotlib.axes._axes import _log as matplotlib_axes_logger
from IPython.display import HTML

# %% [markdown]
# ### Определения констант и функций <a name='const'/>
# %% [markdown]
# #### Функции для работы с базой данных <a name='db_functions'/>
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
    Возвращает: список Named Tuple-ов — результат запроса. Если результатов
    нет, будет возвращён None.
    """
    query = _format_select(ctes, select)
    # print("*DBG* multiline_query will execute:\n" + query)
    cursor.execute(query)
    return cursor.fetchall()

# %%
# Выполнение большого запроса в базу так, чтобы не сожрать всю память.
# Решение со [StackOverflow](https://stackoverflow.com/a/57015290)
# SA: https://docs.python.org/3/library/itertools.html#itertools.chain,
#     https://docs.python.org/3.5/library/functools.html#functools.partial
def large_query(cursor: "DB Cursor", ctes: "list of CTEs", sql: "SQL query",
               chunksize=1000) -> chain:
    query = _format_select(ctes, sql)
    cursor.execute(query)
    chunks_until_empty = iter(partial(cursor.fetchmany, chunksize), [])
    return chain.from_iterable(chunks_until_empty)

# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#program)  |
# |:---|---:|

# %% [markdown]
# #### Функции, связанные с интерактивной работой в Jupyter Notebook <a name='nb_functions'/>
# %% [markdown]
# Определение, работаем ли мы в iPython (jupyter использует его же) или в стандартном
# интерпретаторе, то есть в пакетном режиме.
# %%
def is_interactive() -> bool:
    try:
        __ = get_ipython()
    except NameError:
        return False
    return True

# %% [markdown]
# Инициализация графической подсистемы, только в интерактивном режиме
# %%
def init_graphics():
    sns.set()
    matplotlib_axes_logger.setLevel('ERROR')  # disable color warnings in Matplotlib
    sns.set_style('whitegrid')
    plt.ioff()
    return

# %% [markdown]
# Функция для интерактивного режима, печатает самые популярные и самые НЕпопулярные курсы и пары курсов.
# Сложная внутренняя структура ради [красивых рамочек](#tops_by_popularity)
# %%
def print_tops(ids_count, pairs_count):
    """Печатает верхние и нижние N курсов и пар. Параметры: 1) счётчик курсов, 2) счётчик пар.
       Количеством курсов в выдаче можно поиграть"""
    TOP_COUNT = 5
    IDS_LEN   = TOP_COUNT*5 + (TOP_COUNT-1) * 3 + 2
    PAIRS_LEN = TOP_COUNT*10 + (TOP_COUNT-1) * 3 + 2
    # Определим пару внутренних функций для упрощения себе жизни. Они печатают красивую
    # табличку с первыми элементами списка.  Длина таблички расчитана на 5 элементов.
    def _print_ids_top(header: str, ids_top: "list of tuples: (int, int)"):
        print(header + "\n +" + IDS_LEN * "-" + "+\n",
              "| " + " | ".join(["{:5d}".format(k) for (k,v) in ids_top]) + " |\n",
              "+"  + IDS_LEN * "-" + "+\n")

    def _st2str (aset: set, sep=", ") -> str:
        """Форматирует сет чисел как строку, где эти числа перечислены через разделитель"""
        return sep.join([str(i) for i in aset])

    def _print_pairs_top(header: str, pairs_top: "list of tuples (frozenset, int)"):
        print(header + "\n +" + PAIRS_LEN * "-" + "+\n",
              "| " + " | ".join([ f"{_st2str(st):10s}" for (st, cnt) in pairs_top ]) + " |\n",
              "+" + PAIRS_LEN * "-" + "+\n")

    _print_ids_top(f"Верхние {TOP_COUNT} самых покупаемых курсов",ids_count.most_common(TOP_COUNT))
    _print_pairs_top(f"{TOP_COUNT} самых покупаемых пар курсов", pairs_count.most_common(TOP_COUNT))
    _print_ids_top(f"Нижние {TOP_COUNT} самых НЕпопулярных курсов", ids_count.most_common()[-TOP_COUNT-1:-1])
    _print_pairs_top(f"{TOP_COUNT} самых НЕпопулярных пар курсов", pairs_count.most_common()[-TOP_COUNT-1:-1])
    display(HTML('''<a name="tops_by_popularity"/>'''))
    return

# %% [markdown]
# Наглядное представление (не)популярности курсов: построение количества проданных курсов
# на координатной плоскости по убыванию.
# %%
def plot_top_courses(ids_count: 'Counter of course IDs and buy counts'):
    """Строит график количества заказов на курсы, начиная с самых популярных.
    Масштаб по оси Y логарифмический."""
    pop_courses = pd.Series({id: cnt for id, cnt in ids_count.most_common()}) 
    fig = plt.figure(figsize=(12,7)) 
    ax = pop_courses.plot(kind='bar')
    ax.set_title("Популярность курсов, от самых популярных к малоизвестным")
    plt.yscale('log')
    plt.xticks([])
    plt.show()
    display(HTML('''
    <p>Логарифмический масштаб помог выявить на графике три группы курсов:</p>
    <ol>
    <li><p>Два-три самых популярных (551, 566 и, возможно, 515).</p></li>
    <li><p>Основная часть курсов находится на экспоненте, которая спускается от тысячи заказов примерно до 50-ти.</p></li>
    <li><p>Малопопулярные курсы, каждый из которых нашёл себе менее 50 покупателей. Таких немного, около двадцати,
    но с ними нужно что-то решать: или это курсы, которые только начали предлагатся, и нужно их рекламировать,
    или это курсы, популярность которых мала по другим причинам —- и нужно разбираться, почему их не покупают.</p></li>
    </ol>'''))

# %% [markdown]
# Таблица 126×126 слишком велика для удобного отображения на экране, но если
# преобразовать её в [тепловую карту](#pairs_heatmap_a), её можно воспринимать.  Карта, конечно,
# будет симметричной относительно диагонали, которая идёт от "частого" угла (левого
# верхнего) в "редкий" (правый нижний).
# %%
def show_heatmap_pairs(course_pairs_df: "2D matrix of pairs frequency"):
    fig, (ax_norm, ax_log) = plt.subplots(1,2, figsize=(14,6))
    fig.suptitle('Популярность пар курсов. По осям популярность уменьшается слева направо и сверху вниз')
    # color_norm = mpl.colors.Normalize(0,400)
    gn = sns.heatmap(course_pairs_df, annot=False,
        norm=mpl.colors.Normalize(0,400),
        linewidths=0.0,
        cmap=sns.color_palette('OrRd', 400),
        ax = ax_norm)
    gn.set_title("Цвет в линейном масштабе")
    gn.set_xticks([])
    gn.set_yticks([])
    # Поскольку в логарифмическом масштабе нельзя изобразить ноль,
    # добавляю фиктивные 0.1 раза встречаемости во все пары.  Это влияет только
    # на изображение.
    gl = sns.heatmap(course_pairs_df+0.1, annot=False,
        norm=mpl.colors.LogNorm(0.1,700),
        linewidths=0.0,
        cmap=sns.color_palette('OrRd', 400),
        ax = ax_log)
    gl.set_title("Цвет в логарифмическом масштабе")
    gl.set_xticks([])
    gl.set_yticks([])
    plt.show()
    display(HTML('<a name="pairs_heatmap_a"/>'))
    return

# %% [markdown]
# Популярность пар курсов оказалась крайне неравномерной, и подтверждается предположение о том, что самые
# популярные пары, в общем, составлены из самых популярных курсов.

# %% [markdown]
# [Посмотрим в цифрах](#tb_corners) «частый» (левый верхний) угол и «редкий» (правый нижний) углы матрицы
# %%
def print_top_bottom_corners(course_pairs_df: "2D matrix of course pairs frequency"):
    TOP_ANGLE_COUNT=20
    display(HTML('<a name="tb_corners"/>'))
    print(f"Пары для {TOP_ANGLE_COUNT} самых часто покупаемых курсов")
    display(course_pairs_df.iloc[0:TOP_ANGLE_COUNT,0:TOP_ANGLE_COUNT])
    print(f"Пары для {TOP_ANGLE_COUNT} редко покупаемых курсов")
    MINUS_TOP = (-1) * TOP_ANGLE_COUNT - 1
    display(course_pairs_df.iloc[MINUS_TOP:-1,MINUS_TOP:-1])
    return

# %% [markdown]
# Как и следовало ожидать, самые частые сочетания получаются из часто покупаемых
# курсов, редко покупаемые курсы и пары образуют нечасто.

# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#program)  |
# |:---|---:|

# %% [markdown]
# #### Константы для создания SQL запросов  <a name='const_cte'/>

# %% [markdown]
# Определю некоторое количество констант, в которых сохраню CTE.
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
# Ответ "Недостаточно данных, но могу посмотреть количество разных курсов в корзинах за 2 года"

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
# Все проданные курсы по убыванию популярности. Учитываются и одиночные продажи, и многократные.
# (т. е. пользователь может купить один курс, а может 20)
# %%
TIMES_BOUGHT_BY_COURSE = """\
times_bought_by_resid as (
    select course_id, count(distinct user_id) as times_bought
    from user_course_pairs
    group by course_id
    order by times_bought desc
)
"""
 
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
# | [К оглавлению](#contents) | [К началу раздела](#program)  |
# |:---|---:|
# 

# %% [markdown]
# #### Функции для получения конкретных данных из базы <a name='data_gather'/>
# %% [markdown]
# Запускаю некоторые запросы, чтобы убедиться, что у меня CTE составлены правильно и выдаются ожидаемые
# результаты, которые я уже получил из базы вручную, в процессе отладки CTE.
# %%
def check_ctes(cursor):
    """Проверка, что с CTE всё хорошо, они составлены правильно и те результаты, которые возвращают
    запросы, совпадают с ожидаемыми.  Работает только для данных 2017-2018 годов, для других данных
    нужно переделать или отключить.
    Параметры: 1) Курсор PgSQL"""
    bought_courses_cnt =  psql_query(cursor, [USER_COURSE_PAIRS, COURSES_BOUGHT], 'select count(course_id) from courses_bought')[0][0]
    bought_courseids_lst = psql_query(cursor, [USER_COURSE_PAIRS, COURSES_BOUGHT], 'select course_id from courses_bought')

    assert(psql_query(cursor, [USER_COURSE_PAIRS, BUYERS_COUNT], "select * from buyers_count")[0][0] == 49006)
    assert(psql_query(cursor, [COURSES_IN_CARTS], 'select * from courses_count')[0][0] == 127)
    assert(bought_courses_cnt == 126)
    assert(len(bought_courseids_lst) == 126)
    return True

# %% [markdown]
# Запрос в базу: список курсов и число приобретений этих курсов клиентами
# по убыванию популярности.  Курсов немного, всю информацию получаю в один запрос.
# %%
def get_cids_by_popularity(cursor: "PsycpPg2 database cursor") -> Counter:
    ids_count = Counter()
    for id, cnt in psql_query(cursor, [USER_COURSE_PAIRS, TIMES_BOUGHT_BY_COURSE],
                                """select * from times_bought_by_resid;"""):
        ids_count[id] = cnt
    return ids_count

# %% [markdown]
# Функция, которая получает список курсов, купленных каждым клиентом, из базы.
# Интересен случай, когда пользователь покупал одни и те же курсы больше одного раза.
# Решаем эту проблему как в SQL c помощью `distinct`, так и здесь — использованием `set`.
# %%
def get_ids_pairs_counts_from_db(cursor) -> (Counter, Counter):
    """Функция делает запрос в базу и возвращает два счётчика в кортеже:
    1) Счётчик встречаемости курсов (по ID) среди покупателей, которые приобрели 2 и более курсов.
    2) Счётчик встречаемости пар курсов (каждой паре поставлено в соответствие кол-во её вхождений
    в покупках пользователей.  Группировка по пользователям, так что курсы, купленные в разных
    корзинах одним пользователем, окажутся в паре)"""
    user_count=0
    users_data_d = {}  # {uid: { {course, course}, {course, course}, ... } }
    all_courses = set()
    pairs_count = Counter()
    ids_count = Counter()

    for (users_count, (user_id, courses_cnt, user_courses_str)) in enumerate(
        large_query(cursor, [USER_COURSE_PAIRS], COURSES_LIST_QUERY)
        ):
        s_courses = set( [int(i) for i in user_courses_str.split()] )  # default split on whitespace
        assert(len(s_courses) == courses_cnt)
        # increase counters for individual courses
        # for course_id in s_courses:
        #     ids_count[course_id] += 1
        l_pairs = combinations(s_courses, 2)
        users_data_d[user_id] = set([frozenset(t) for t in l_pairs])
        for pair in users_data_d[user_id]:
            pairs_count[pair] += 1
    return pairs_count


# %% [markdown]
# Занятно, что в списке возможных комбинаций мы получаем
# [полный граф](https://ru.wikipedia.org/wiki/%D0%9F%D0%BE%D0%BB%D0%BD%D1%8B%D0%B9_%D0%B3%D1%80%D0%B0%D1%84)
# из N вершин, где N - количество купленных заказчиком курсов.

# %% [markdown]
# Общее количество найденных пар курсов: 3989 (это длина счётчика пар), это означает, что матрица сочетаний курсов
# получается довольно «рыхлой»: всего возможно 126×126, то есть 15876 сочетаний, наблюдается примерно четверть.
# Самая популярная пара 566+551.
# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#program)  |
# |:---|---:|

# %% [markdown]
# #### Построение матрицы сочетаний курсов <a name='matrix_create'/>

# %% [markdown]
# Интересный момент — что делать, если в пару рекомендуемых курсов попадает курс их «хвоста» распределения, т. е.
# малопопулярный.  Возможно, есть смысл предлагать в этом случае самые популярные курсы?  Тут бы была полезна
# модификация исходных данных - система тегов, чтобы не предлагать в пару к курсу по Python курс по разведению
# роз.  Будем рабоать с тем, что есть.
# 
# Нам понадобится какая-то точка отсчёта для «непопулярных» или, другими словами, редко встречающихся
# курсов. В качестве такой точки возьму 5% квантиль популярности курса.
# %%
def get_unpopular_threshold(freq_table) -> int:
    "Выдаёт порог количества покупок, ниже которого курс считается непопулярным"
    # print('*DBG* Low popularity threshold:', math.ceil(freq_table.quantile(0.05)))
    return math.ceil(freq_table.quantile(0.05))

# %% [markdown]
# Функция для построения двумерной матрицы из таблицы частоты встречаемости пар курсов
#
# Имеем: таблицу pairs_count, где каждой паре поставлена в соответсвие её частота встречаемости.
# Строю: таблицу, где ID курсов по вертикали и горизонтали (их не так много). Делаю по обеим осям
# сортировку по частоте курсов: логично предположить, что те курсы, которые покупаются чаще всего,
# будут образовывать и наиболее часто встрачающиеся пары.

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
# #### Построение таблицы рекомендаций <a name='recommendations'/>
 
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

# %% [markdown]
# Функция возвращает датафрейм с рекомендуемыми курсами. Структура датафрейма: индекс - курс,
# к которому даётся рекомендация, первая колонка (first_rec) — первая рекомендация, вторая
# колонка — вторая рекомендация.  Например, если результат вызова сохранён в переменной `res`, то
# для получения обеих рекомендаций к курсу 489 нужно вызвать: `res.loc[489]`, для получения
# только первой — `res.loc[489, 'first_rec']`, для второй —`res.loc[289, 'second_rec']`.
# %%
def get_recommended_courses(pairs_df, freq_courses) -> pd.DataFrame:
    """ Функция строит массив рекомендаций по данным, и таблице частоты пар курсов.
    Параметры:
        1) DataFrame пар курсов, где идентификаторы по вертикали и горизонтали, количество пар на пересечениях.
        2) Series, в которой собраны курсы по порядку убывания популярности.
    Возвращает:
        pd.DataFrame с ID курса в индексе, рекомендациями в колонках 'first_rec' и 'second_rec'.
        Сортировка по обеим осям по убыванию популярности курсов
    """
    threshold = get_unpopular_threshold(freq_table)
    res = pd.DataFrame(index=pairs_df.index, dtype=np.uint32)
    for (row_idx, courses_paired) in pairs_df.iterrows(): 
        max_two = courses_paired.sort_values(ascending=False)[0:2]
        # И тут вспоминаю, что есть ограничение "курс слишком непопулярный". 
        # У нас max_two.iloc[0] >= max_two.iloc[1]
        if max_two.iloc[1] > threshold:
            res.loc[row_idx, 'first_rec']  = max_two.index[0]
            res.loc[row_idx, 'second_rec'] = max_two.index[1]
        elif max_two.iloc[0] > threshold:
            res.loc[row_idx, 'first_rec']  = max_two.index[0]
            res.loc[row_idx, 'second_rec'] = freq_courses.index[0]
        else:
            res.loc[row_idx, 'first_rec']  = freq_courses.index[0]
            res.loc[row_idx, 'second_rec'] = freq_courses.index[1]
    return res.astype(np.uint32)

# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#program)  |
# |:---|---:|
# %% [markdown]
# #### Функция для интерактивной работы (Jupyter notebook или iPython) <a name='interactive'/>
# %%
def interactive_work(course_pairs_df):
    """Программа работает в интерактивном режиме. Выводим все данные в ноутбук,
    строим графики и т. п."""
    init_graphics()
    print_tops(ids_count, pairs_count)
    plot_top_courses(ids_count)
    show_heatmap_pairs(course_pairs_df)
    res = get_recommended_courses(course_pairs_df, freq_table)
    print("Можно вывести обе рекомендации для одного или нескольких курсов таблицей:\n",
           res.loc[489])
    print("Или использовать конкретные поля, например, так:" + "\n" +
          "Для курса 489 первая рекомендация: {0} и вторая {1}".format(
          res.loc[489, 'first_rec'], res.second_rec[489]
          ))
    return

# %% [markdown]
# #### Функция для пакетной работы <a name='batch'/>
# %% [markdown] 
# Выдача рекомендаций в CSV формате на стандартный вывод.
# %%
def packet_job(course_pairs_df):
    """Программа вызвана в пакетном режиме, печать таблицы на STDOUT и выход"""
    print(get_recommended_courses(course_pairs_df, freq_table).sort_index().to_csv(index_label="course_ID"))
    return

# %% [markdown]
# ### Основная точка входа в программу <a name='exec_point'/>
# %% [code]
if __name__ == "__main__":
    cursor = init_connect(DB_CONNECT_STRING)
    check_ctes(cursor)
    ids_count = get_cids_by_popularity(cursor)
    pairs_count = get_ids_pairs_counts_from_db(cursor)
    # Чтобы номера курсов были в индексе, а количество в значении, пары нужно перевести в словарь
    freq_table = pd.Series({k:v for k,v in ids_count.most_common()})
    course_pairs_df = make_freq_matrix(pairs_count)
    if is_interactive():
        display(HTML('<a name="output"/>'))  # anchor for links
        interactive_work(course_pairs_df)
    else:
        packet_job(course_pairs_df)

# %% [markdown]
# | [К оглавлению](#contents) | [К началу раздела](#program)  |
# |:---|---:|
