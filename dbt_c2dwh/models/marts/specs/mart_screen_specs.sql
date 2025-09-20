select a.sku,
	a.name,
    a.category,
    a.brand,
	{{build_specs_query(category='screen',alias='b')}}
from {{ ref('dim_product') }} a
	left join {{ ref('dim_specs') }} b on a.sku = b.product_sku
where a.category = 'Screen'
group by a.sku, a.name, a.category, a.brand