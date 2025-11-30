
--для работы используем таблицы из hw_2_stg 


--Запрос 1. Вывести распределение (количество) клиентов по сферам деятельности, 
--отсортировав результат по убыванию количества.
select 
    job_industry_category as industry_category,
    count(*) as customer_count
from hw_2_stg.customer c
where job_industry_category is not null
group by job_industry_category
order by customer_count desc;


--Запрос 2. Найти общую сумму дохода (list_price*quantity) по всем подтвержденным заказам 
--за каждый месяц по сферам деятельности клиентов. 
--Отсортировать результат по году, месяцу и сфере деятельности.
select 
    extract(year from o.order_date) as year,
    extract(month from o.order_date) as month,
    c.job_industry_category as industry_category,
    sum(pn.list_price * oi.quantity) as total_revenue
from hw_2_stg.orders o
    inner join hw_2_stg.order_items oi on o.order_id = oi.order_id
    inner join hw_2_stg.product_new pn on oi.product_id = pn.product_id
    inner join hw_2_stg.customer c on o.customer_id = c.customer_id
where o.order_status = 'Approved'
group by 
    extract(year from o.order_date),
    extract(month from o.order_date),
    c.job_industry_category
order by 
    year,
    month,
    industry_category;


--Запрос 3. Вывести количество уникальных онлайн-заказов для всех брендов в рамках подтвержденных заказов 
--клиентов из сферы IT. Включить бренды, у которых нет онлайн-заказов от IT-клиентов, 
--для них должно быть указано количество 0.
with it_online_orders as (
    select 
        pn.brand,
        count(distinct o.order_id) as order_count
    from hw_2_stg.orders o
        inner join hw_2_stg.order_items oi on o.order_id = oi.order_id
        inner join hw_2_stg.product_new pn on oi.product_id = pn.product_id
        inner join hw_2_stg.customer c on o.customer_id = c.customer_id
    where o.order_status = 'Approved'
        and o.online_order is true
        and c.job_industry_category = 'IT'
    group by pn.brand
),
all_brands as (
    select distinct brand
    from hw_2_stg.product_new
)
select 
    ab.brand as brand,
    coalesce(ioo.order_count, 0) as unique_online_order_count
from all_brands ab
    left join it_online_orders ioo on ab.brand = ioo.brand
order by ab.brand;


--Запрос 4. Найти по всем клиентам: сумму всех заказов (общего дохода), максимум, минимум и количество заказов, 
--а также среднюю сумму заказа по каждому клиенту. 
--Отсортировать результат по убыванию суммы всех заказов и количества заказов. 
--Выполнить двумя способами: используя только GROUP BY и используя только оконные функции. Сравнить результат.

--Способ 1: используя только GROUP BY
with customer_order_totals as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        sum((oi.item_list_price_at_sale * oi.quantity)::numeric)::numeric as order_sum
    from hw_2_stg.customer c
        left join hw_2_stg.orders o on c.customer_id = o.customer_id
        left join hw_2_stg.order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name, o.order_id
)
select 
    customer_id,
    first_name,
    last_name,
    sum(order_sum)::numeric as total_order_sum,
    max(order_sum)::numeric as max_order,
    min(order_sum)::numeric as min_order,
    count(order_id) as order_count,
    avg(order_sum)::numeric as avg_order_sum
from customer_order_totals
group by customer_id, first_name, last_name
order by total_order_sum desc nulls last, order_count desc nulls last;

--Способ 2: используя только оконные функции
with customer_order_items as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        (oi.item_list_price_at_sale * oi.quantity)::numeric as position_sum
    from hw_2_stg.customer c
        left join hw_2_stg.orders o on c.customer_id = o.customer_id
        left join hw_2_stg.order_items oi on o.order_id = oi.order_id
),
customer_order_totals_raw as (
    select 
        customer_id,
        first_name,
        last_name,
        order_id,
        position_sum,
        sum(position_sum) over(partition by customer_id, order_id)::numeric as order_sum,
        row_number() over(partition by customer_id, order_id order by customer_id, order_id nulls last) as rn
    from customer_order_items
),
customer_order_totals as (
    select 
        customer_id,
        first_name,
        last_name,
        order_id,
        order_sum
    from customer_order_totals_raw
    where rn = 1
),
customer_stats as (
    select 
        customer_id,
        first_name,
        last_name,
        sum(order_sum) over(partition by customer_id)::numeric as total_order_sum,
        max(order_sum) over(partition by customer_id)::numeric as max_order,
        min(order_sum) over(partition by customer_id)::numeric as min_order,
        count(order_id) over(partition by customer_id) as order_count,
        avg(order_sum) over(partition by customer_id)::numeric as avg_order_sum,
        row_number() over(partition by customer_id order by customer_id) as rn_cust
    from customer_order_totals
)
select 
    customer_id,
    first_name,
    last_name,
    total_order_sum,
    max_order,
    min_order,
    order_count,
    avg_order_sum
from customer_stats
where rn_cust = 1
order by total_order_sum desc nulls last, order_count desc nulls last;


--Сравнение результатов двух способов (должен вернуть пустой результат, если результаты идентичны):
(
    --Способ 1: GROUP BY
with customer_order_totals as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        sum((oi.item_list_price_at_sale * oi.quantity)::numeric)::numeric as order_sum
    from hw_2_stg.customer c
        left join hw_2_stg.orders o on c.customer_id = o.customer_id
        left join hw_2_stg.order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name, o.order_id
)
select 
    customer_id,
    first_name,
    last_name,
    sum(order_sum)::numeric as total_order_sum,
    max(order_sum)::numeric as max_order,
    min(order_sum)::numeric as min_order,
    count(order_id) as order_count,
    avg(order_sum)::numeric as avg_order_sum
from customer_order_totals
group by customer_id, first_name, last_name
order by total_order_sum desc nulls last, order_count desc nulls last
)
except all
(
    --Способ 2: Оконные функции
with customer_order_items as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        (oi.item_list_price_at_sale * oi.quantity)::numeric as position_sum
    from hw_2_stg.customer c
        left join hw_2_stg.orders o on c.customer_id = o.customer_id
        left join hw_2_stg.order_items oi on o.order_id = oi.order_id
),
customer_order_totals_raw as (
    select 
        customer_id,
        first_name,
        last_name,
        order_id,
        position_sum,
        sum(position_sum) over(partition by customer_id, order_id)::numeric as order_sum,
        row_number() over(partition by customer_id, order_id order by customer_id, order_id nulls last) as rn
    from customer_order_items
),
customer_order_totals as (
    select 
        customer_id,
        first_name,
        last_name,
        order_id,
        order_sum
    from customer_order_totals_raw
    where rn = 1
),
customer_stats as (
    select 
        customer_id,
        first_name,
        last_name,
        sum(order_sum) over(partition by customer_id)::numeric as total_order_sum,
        max(order_sum) over(partition by customer_id)::numeric as max_order,
        min(order_sum) over(partition by customer_id)::numeric as min_order,
        count(order_id) over(partition by customer_id) as order_count,
        avg(order_sum) over(partition by customer_id)::numeric as avg_order_sum,
        row_number() over(partition by customer_id order by customer_id) as rn_cust
    from customer_order_totals
)
select 
    customer_id,
    first_name,
    last_name,
    total_order_sum,
    max_order,
    min_order,
    order_count,
    avg_order_sum
from customer_stats
where rn_cust = 1
order by total_order_sum desc nulls last, order_count desc nulls last
);


--Запрос 5. Найти имена и фамилии клиентов с топ-3 минимальной и топ-3 максимальной суммой транзакций 
--за весь период (учесть клиентов, у которых нет заказов, приняв их сумму транзакций за 0).
with customer_transaction_sums as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        coalesce(sum(oi.item_list_price_at_sale * oi.quantity), 0) as transaction_sum
    from hw_2_stg.customer c
        left join hw_2_stg.orders o on c.customer_id = o.customer_id
        left join hw_2_stg.order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name
),
top_3_min as (
    select 
        customer_id,
        first_name,
        last_name,
        transaction_sum,
        'min' as type
    from customer_transaction_sums
    order by transaction_sum asc
    limit 3
),
top_3_max as (
    select 
        customer_id,
        first_name,
        last_name,
        transaction_sum,
        'max' as type
    from customer_transaction_sums
    order by transaction_sum desc
    limit 3
)
select 
    first_name,
    last_name,
    transaction_sum,
    type
from top_3_min
union all
select 
    first_name,
    last_name,
    transaction_sum,
    type
from top_3_max
order by type, transaction_sum;


--Запрос 6. Вывести только вторые транзакции клиентов (если они есть) с помощью оконных функций. 
--Если у клиента меньше двух транзакций, он не должен попасть в результат.
with customer_orders_ranked as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        row_number() over(partition by c.customer_id order by o.order_date, o.order_id) as order_number,
        count(*) over(partition by c.customer_id) as total_order_count
    from hw_2_stg.customer c
        inner join hw_2_stg.orders o on c.customer_id = o.customer_id
)
select 
    customer_id,
    first_name,
    last_name,
    order_id,
    order_date
from customer_orders_ranked
where order_number = 2
    and total_order_count >= 2
order by customer_id;


--Запрос 7. Вывести имена, фамилии и профессии клиентов, а также длительность максимального интервала 
--(в днях) между двумя последовательными заказами. Исключить клиентов, у которых только один или меньше заказов.
with customer_orders_dates as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.job_title,
        o.order_id,
        o.order_date,
        lag(o.order_date) over(partition by c.customer_id order by o.order_date) as previous_order,
        count(*) over(partition by c.customer_id) as order_count
    from hw_2_stg.customer c
        inner join hw_2_stg.orders o on c.customer_id = o.customer_id
),
customer_intervals as (
    select 
        customer_id,
        first_name,
        last_name,
        job_title,
        order_count,
        order_date - previous_order as interval_days
    from customer_orders_dates
    where previous_order is not null
        and order_count > 1
),
max_intervals as (
    select 
        customer_id,
        first_name,
        last_name,
        job_title,
        max(interval_days) as max_interval_days
    from customer_intervals
    group by customer_id, first_name, last_name, job_title
)
select 
    first_name,
    last_name,
    job_title as job_title,
    max_interval_days
from max_intervals
order by max_interval_days desc, first_name, last_name;


--Запрос 8. Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния (wealth_segment). 
--Вывести имя, фамилию, сегмент и общий доход. Если в сегменте менее 5 клиентов, вывести всех.
with customer_revenue as (
    select 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.wealth_segment,
        coalesce(sum(oi.item_list_price_at_sale * oi.quantity), 0) as total_revenue
    from hw_2_stg.customer c
        left join hw_2_stg.orders o on c.customer_id = o.customer_id
        left join hw_2_stg.order_items oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name, c.wealth_segment
),
ranked_customers as (
    select 
        customer_id,
        first_name,
        last_name,
        wealth_segment,
        total_revenue,
        row_number() over(partition by wealth_segment order by total_revenue desc) as rank
    from customer_revenue
)
select 
    first_name,
    last_name,
    wealth_segment as wealth_segment,
    total_revenue
from ranked_customers
where rank <= 5
order by wealth_segment, total_revenue desc;

