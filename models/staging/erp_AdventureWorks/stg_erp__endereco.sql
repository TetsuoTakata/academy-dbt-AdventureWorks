with
    fonte_endereco as (
        select
            cast(addressid as integer) as id_endereco
            , cast(stateprovinceid as integer) as id_provincia_estado
            , cast(addressline1 as string) as endereco1
            , cast(addressline2 as string) as endereco2
            , cast(city as string) as cidade
            , cast(postalcode as string) as codigo_postal
        from {{ source('erp', 'address') }}
    )
select *
from fonte_endereco