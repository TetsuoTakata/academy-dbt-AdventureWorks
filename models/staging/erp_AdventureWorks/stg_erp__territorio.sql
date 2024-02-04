with
    fonte_territorio as (
        select
            cast (territoryid as integer) as id_territorio
            , cast (name as string) as territorio
            , cast (countryregioncode as string) as codigo_pais
        from {{ source('erp', 'salesterritory') }}
    )
select *
from fonte_territorio