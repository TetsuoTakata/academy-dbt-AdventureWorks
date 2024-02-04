with
    fonte_provincia_estado as (
        select
            cast (stateprovinceid as integer) as id_provincia_estado
            , cast (territoryid as integer) as id_territorio
            , cast (stateprovincecode as string) as codigo_provincia_estado
            , cast (countryregioncode as string) as codigo_regiao_pais
            , cast (name as string) as provincia_estado
        from {{ source('erp', 'stateprovince') }}
    )
select *
from fonte_provincia_estado