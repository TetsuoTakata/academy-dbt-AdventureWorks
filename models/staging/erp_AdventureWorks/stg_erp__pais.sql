with
    fonte_pais as (
        select
            cast (countryregioncode as string) as codigo_regiao_pais
            , cast (name as string) as pais
        from {{ source('erp', 'countryregion') }}
    )
select *
from fonte_pais