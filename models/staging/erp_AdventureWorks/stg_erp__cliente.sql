with
    fonte_cliente as (
        select 
            cast(customerid as integer) as id_cliente
            , cast(personid as integer) as id_vendedor
            , cast(storeid as integer) as id_loja
            , cast(territoryid as integer) as id_territorio
        from {{ source('erp', 'customer') }}
    )
select *
from fonte_cliente