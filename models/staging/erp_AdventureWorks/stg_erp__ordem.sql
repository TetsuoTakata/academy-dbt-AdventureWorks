with
    fonte_ordem as (
        select
            cast (salesorderid as integer) as id_venda
            , cast (customerid as integer) as id_cliente
            , cast (territoryid as integer) as id_territorio
            , cast (salespersonid as integer) as id_vendedor
            , cast (shiptoaddressid as integer) as id_endereco
            , cast (creditcardid as integer) as id_cartao_credito
            , cast (subtotal as numeric) as subtotal
            , cast (taxamt as numeric) as taxa
            , cast (freight as numeric) as frete
            , cast (totaldue as numeric) as total
            , date (orderdate) as data_pedido
            , cast (status as integer) as status
        from {{ source('erp', 'salesorderheader') }}
    )

select *
from fonte_ordem
