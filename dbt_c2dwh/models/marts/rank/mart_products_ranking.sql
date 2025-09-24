with latest_date as(
	select cast(full_date as varchar)
	from {{ ref('dim_date') }}
	order by id desc
	limit 1
)
select a.sku,
	a.name,
	a.category,
	a.brand,
	round(avg(b.rating), 2) avg_rating,
	sum(
		case
			when b.partition_date = (
				select *
				from latest_date
			) then b.reviews_count
		end
	) total_reviews,
	cast(current_timestamp as timestamp) last_update
from {{ ref('dim_product') }} a
	left join {{ ref('fact_reviews') }} b on a.sku = b.product_sku
where a.category != ''
group by a.sku,
	a.name,
	a.category,
	a.brand
order by avg_rating desc,
	total_reviews desc