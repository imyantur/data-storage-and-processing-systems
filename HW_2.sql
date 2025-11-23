--создание схемы для второй домашки
create schema if not exists hw_2_stg;

--заливаем таблицы через импорт данных
--проверка созданных таблиц
select * from hw_2_stg.orders o;
select * from hw_2_stg.customer c;
select * from hw_2_stg.product p;
select * from hw_2_stg.order_items oi;
--все таблицы корректно импортировались


--ввиду использования запросов с джойнами по ключам производим дедубликацию таблиц (если есть)
--видим дубликаты по атрибуту product_id в таблице product
select product_id, count(*)
from hw_2_stg.product p
group by product_id
having count(*) > 1
order by p.product_id;

--дедубликация - берем первую запись каждого id продукта из таблицы
create table hw_2_stg.product_new as 
with data_rn as (
select *, row_number() over(partition by p.product_id) as rn 
from hw_2_stg.product p
)
select product_id, brand, product_line, product_class, product_size, list_price, standard_cost
from data_rn
where rn = 1;
--получаем 101 уникальных product_id

--таблица customer дублей по id нет
select c.customer_id , count(*)
from hw_2_stg.customer c 
group by c.customer_id 
having count(*) > 1
order by c.customer_id ;


--таблица orders дублей по id нет
select c.order_id  , count(*)
from hw_2_stg.orders c 
group by c.order_id 
having count(*) > 1
order by c.order_id ;

--таблица order_items дублей по id нет
select oi.order_item_id, count(*)
from hw_2_stg.order_items oi  
group by oi.order_item_id
having count(*) > 1
order by oi.order_item_id ;



--Запрос 1. Вывести все уникальные бренды, у которых есть хотя бы один продукт со стандартной стоимостью выше 1500 долларов, 
--и который был продан как минимум 1000 раз (суммарное количество)

with 
targer_product_list as (
select product_id, sum(oi.quantity) as sum_quantity
from hw_2_stg.product_new pn
	left join hw_2_stg.order_items oi 
		using(product_id)
where pn.standard_cost > 1500
group by product_id
having sum(oi.quantity) >= 1000
)
select pn.brand
from targer_product_list tpl 
	left join hw_2_stg.product_new pn 
		using(product_id)
group by pn.brand;


--Запрос 2. Для каждого дня в диапазоне с 2017-04-01 по 2017-04-09 
--включительно вывести количество подтвержденных онлайн-заказов и количество уникальных клиентов, совершивших эти заказы
select order_date, count(*) as orders_count, count(distinct customer_id) as count_customers
from hw_2_stg.orders o 
where order_date >= '2017-04-01' and order_date <= '2017-04-09'
	and order_status = 'Approved'
	and online_order is TRUE
group by order_date
order by o.order_date;

--Запрос 3. Вывести профессии для клиентов, которые: находятся в сфере 'IT' 
--И их профессия начинается с Senior, находятся в сфере 'Financial Services' и их профессия начинается с Lead. 
--При этом для обоих пунктов учесть, что возраст клиентов должен быть старше 35 лет. Использовать UNION ALL для объединения 2 пунктов
select distinct job_title
from hw_2_stg.customer
where job_industry_category = 'IT'
	and job_title ilike 'senior%'
	and extract(year from age(CURRENT_DATE, dob::date)) > 35
union all
select distinct job_title
from hw_2_stg.customer
where job_industry_category = 'Financial Services'
	and job_title ilike 'lead%'
	and extract(year from age(CURRENT_DATE, dob::date)) > 35;


--Запрос 4. Вывести бренды, которые были куплены клиентами из сферы Financial Services, но НЕ были куплены клиентами из сферы IT
with ttl_data as (
select o.order_id, oi.product_id, p.brand, c.customer_id, c.job_industry_category
from hw_2_stg.orders o
	left join hw_2_stg.order_items oi  
		on o.order_id = oi.order_id 
	left join hw_2_stg.product_new p 
		on p.product_id = oi.product_id 
	left join hw_2_stg.customer c 
		on o.customer_id = c.customer_id 
),
subquery_1 as (
select distinct brand
from ttl_data
where job_industry_category = 'Financial Services'
),
subquery_2 as (
select distinct brand
from ttl_data
where job_industry_category = 'IT'
)
select brand
from subquery_1 s1
where not exists
	(select brand 
	 from subquery_2 s2
	 where s1.brand = s2.brand
	);

--вариант 2
with ttl_data as (
select o.order_id, oi.product_id, p.brand, c.customer_id, c.job_industry_category
from hw_2_stg.orders o
	left join hw_2_stg.order_items oi  
		on o.order_id = oi.order_id 
	left join hw_2_stg.product_new p 
		on p.product_id = oi.product_id 
	left join hw_2_stg.customer c 
		on o.customer_id = c.customer_id 
)
select distinct brand
from ttl_data
where job_industry_category = 'Financial Services'
except
select distinct brand  
from ttl_data
where job_industry_category = 'IT';


--Запрос 5. Вывести 10 клиентов (ID, имя, фамилия), которые совершили наибольшее количество онлайн-заказов (в штуках) 
--брендов Giant Bicycles, Norco Bicycles, Trek Bicycles, при условии, 
--что они активны и имеют оценку имущества (property_valuation) выше среднего по их штату
with 
customer_data as (
select customer_id, deceased_indicator, property_valuation, state, 
	avg(property_valuation) over(partition by state) as avg_pv_by_state
from hw_2_stg.customer
),
customer_filter as (
select customer_id
from customer_data
where property_valuation > avg_pv_by_state
	and deceased_indicator = 'N' --активны
),
main_query as (
select o.customer_id, count(distinct o.order_id) as count_order
from hw_2_stg.orders o
	left join hw_2_stg.order_items oi  
		on o.order_id = oi.order_id 
	left join hw_2_stg.product_new p 
		on p.product_id = oi.product_id 
	left join hw_2_stg.customer c 
		on o.customer_id = c.customer_id 
where o.online_order is true --онлайн заказ
	and p.brand in ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles') --список брендов
	--в условии нет пункта про подтвежденные заказы, поэтому не фильтруем по order_status
	and o.customer_id in 
	(
	select customer_id
	from customer_filter cf
	)
group by o.customer_id 
order by count(distinct o.order_id) desc
limit 10
)
select mq.customer_id, c.first_name, c.last_name, mq.count_order
from main_query mq
	left join hw_2_stg.customer c using(customer_id)
order by count_order desc;


--Запрос 6. Вывести всех клиентов (ID, имя, фамилия), у которых нет подтвержденных онлайн-заказов за последний год, 
--но при этом они владеют автомобилем и их сегмент благосостояния не Mass Customer.
with 
customer_with_orders as (
select distinct o.customer_id 
from hw_2_stg.orders o
where o.order_status = 'Approved'
	and o.online_order is true 
	and extract(year from o.order_date) = extract(year from (select max(order_date) from hw_2_stg.orders))
)
select c.customer_id, c.first_name, c.last_name 
from hw_2_stg.customer c
where c.owns_car = 'Yes'
	and c.wealth_segment <> 'Mass Customer'
	and not exists (
		select 1
		from customer_with_orders cwo
		where cwo.customer_id = c.customer_id
	);


--Запрос 7. Вывести всех клиентов из сферы IT (ID, имя, фамилия), которые купили 2 из 5 продуктов с самой высокой list_price в продуктовой линейке Road
with top_5_road_products as (
	select product_id
	from hw_2_stg.product_new
	where product_line = 'Road'
	order by list_price desc
	limit 5
),
customer_purchases as (
	select 
		c.customer_id,
		count(distinct oi.product_id) as products_count
	from hw_2_stg.customer c
		inner join hw_2_stg.orders o on o.customer_id = c.customer_id
		inner join hw_2_stg.order_items oi on oi.order_id = o.order_id
		inner join top_5_road_products t5 on t5.product_id = oi.product_id
	where c.job_industry_category = 'IT'
	group by c.customer_id
	having count(distinct oi.product_id) >= 2
)
select c.customer_id, c.first_name, c.last_name
from customer_purchases cp
	inner join hw_2_stg.customer c on c.customer_id = cp.customer_id;


--Запрос 8. Вывести клиентов (ID, имя, фамилия, сфера деятельности) из сферы IT или Health, которые совершили не менее 3 подтвержденных заказов в период 2017-01-01 по 2017-03-01 и при этом их общий доход от этих заказов превышает 10000 долларов.
--Разделить вывод на две группы (IT и Health) с помощью UNION
with customer_orders_summary as (
	select 
		o.customer_id,
		count(distinct o.order_id) as orders_count,
		sum(oi.item_list_price_at_sale * oi.quantity) as total_revenue
	from hw_2_stg.orders o
		inner join hw_2_stg.order_items oi on oi.order_id = o.order_id
		inner join hw_2_stg.customer c on c.customer_id = o.customer_id
	where o.order_status = 'Approved'
		and o.order_date >= '2017-01-01' 
		and o.order_date < '2017-03-01'
		and c.job_industry_category in ('IT', 'Health')
	group by o.customer_id
	having count(distinct o.order_id) >= 3
		and sum(oi.item_list_price_at_sale * oi.quantity) > 10000
)
select 
	cos.customer_id,
	c.first_name,
	c.last_name,
	c.job_industry_category as industry
from customer_orders_summary cos
	inner join hw_2_stg.customer c on c.customer_id = cos.customer_id
where c.job_industry_category = 'IT'
union
select 
	cos.customer_id,
	c.first_name,
	c.last_name,
	c.job_industry_category as industry
from customer_orders_summary cos
	inner join hw_2_stg.customer c on c.customer_id = cos.customer_id
where c.job_industry_category = 'Health'
order by industry, customer_id;

