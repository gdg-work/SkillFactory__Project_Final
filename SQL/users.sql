-- Сколько клиентов покупали курсы?
-- Каково среднее число купленных курсов на одного клиента? Округлите до двух знаков после точки-разделителя.
with 
    all_data as (
        select *
        from 
            final.carts as c
            join final.cart_items as i
            on c.id = i.cart_id
        order by c.user_id
    ),
    courses_count_by_user as (
        select user_id, count(resource_id) as courses_cnt
        from all_data
        where state = 'successful' and resource_type = 'Course'
        group by user_id
    )
select count(user_id), avg(courses_cnt)
from courses_count_by_user
;

# Сколько клиентов купили больше одного курса?
with 
    all_data as (
        select c.id as cart_id, c.created_at, c.updated_at, purchased_at, state, user_id, 
			    promo_code_id, i.id as item_id, i.created_at as item_added_at,
                i.updated_at as item_updated_at, resource_type, resource_id
        from 
            final.carts as c
            join final.cart_items as i
            on c.id = i.cart_id
        order by c.user_id
    ),
    courses_count_by_user as (
        select user_id, count(resource_id) as courses_cnt
        from all_data
        where state = 'successful' and resource_type = 'Course'
        group by user_id
    )
select user_id, courses_cnt
from courses_count_by_user
where courses_cnt > 1;

-- 
--  Другой вариант:
-- 

-- Сначала получим всех покупателей и курсы, которые были вместе в купленных корзинах.
select user_id, resource_id, resource_type, c.state as course_id
from
	final.carts as c
	join final.cart_items as i
	on c.id = i.cart_id
where
	i.resource_type = 'Course'
    and
    c.state = 'successful'
order by user_id;

-- Теперь, используя это как виртуальную таблицу (CTE), групирую всё добро по пользователю
-- и считаю только количество курсов count-ом, потом будем делать вещи посложнее.
with
    user_course_pairs as (
        -- каждому пользователю ставится в соответствие купленный
        -- им курс. Если курсов больше одного, в выводе будет несколько
        -- записей с одним user_id
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
    ),
    -- сколько клиентов покупали курсы?
    buyers_count as (
        select count(distinct user_id) from user_course_pairs
    ),
    -- сколько всего есть разных курсов?
    -- Ответ "без понятия, но могу посмотреть количество разных купленных курсов за 2 года"
    distinct_courses_count as (
        select count(distinct resource_id)
        from final.cart_items
		where resource_type = 'Course'
    ),
    distinct_courses_bought as (
        select count(distinct course_id)
        from user_course_pairs
    ),
    courses_count_by_user as (
        select
            user_id, count(course_id) as courses_count
        from user_course_pairs
        group by user_id
        order by user_id
    )
select 'User count' as param, count from buyers_count
union all
select
    'Distinct course IDs bought:' as param,
    count
    from distinct_courses_count
union all
select
    'Distinct courses bought by customers:',
    count 
    from distinct_courses_bought
union all
select
    'Customers bought more than one course:' as param,
    count(user_id)
    from courses_count_by_user
    where courses_count > 1;

--
-- Для получения списка курсов по пользователям используем часть этих CTE
with
    user_course_pairs as (
        -- каждому пользователю ставится в соответствие купленный
        -- им курс. Если курсов больше одного, в выводе будет несколько
        -- записей с одним user_id
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
select
	user_id,
	count(distinct course_id) as courses_cnt,
    STRING_AGG(course_id::text, ' ') as courses_list
from user_course_pairs
group by user_id
having count(distinct course_id) > 1;

--
-- Найти список купленных курсов для всех пользователей, кто купил больше одного курса.
with 
    all_data as (
        select c.id as cart_id, c.created_at, c.updated_at, purchased_at, state, user_id, 
			    promo_code_id, i.id as item_id, i.created_at as item_added_at,
                i.updated_at as item_updated_at, resource_type, resource_id
        from 
            final.carts as c
            join final.cart_items as i
            on c.id = i.cart_id
        order by c.user_id
    )
select user_id, STRING_AGG(resource_id::text, '>' order by item_updated_at)
from all_data
where resource_type = 'Course'
group by user_id
having count(distinct resource_id) > 1;

