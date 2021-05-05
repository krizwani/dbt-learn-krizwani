with orders as  (
     select * from {{ ref('stg_orders') }}
 ),

 payments as (
     select * from {{ ref('stg_payments') }}
     where status = 'SUCCESS'
 ),

 joined as (
     select 
     o.order_id,
     o.customer_id,
     sum(p.amount_USD) as amount_USD
     from orders o
     left join payments p on o.order_id = p.order_id
     group by 1,2
 )

 select * from joined