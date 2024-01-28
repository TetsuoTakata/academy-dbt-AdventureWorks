with
    stg_produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )
select *
from stg_produtos