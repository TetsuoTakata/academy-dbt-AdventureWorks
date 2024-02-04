with
    fonte_ordem as (
        select
            cast (salesorderid as integer) as id_venda
            , cast (customerid as integer) as id_cliente
            , cast (territoryid as integer) as id_territorio
            , cast (salespersonid as integer) as id_vendedor
            , cast (billtoaddressid as integer) as id_conta_endereco
            , cast (shiptoaddressid as integer) as id_endereco
            , cast (shipmethodid as integer) as id_metodo_compra
            , cast (creditcardid as integer) as id_cartao_credito
            , cast (subtotal as numeric) as subtotal
            , cast (taxamt as numeric) as taxa
            , cast (freight as numeric) as frete
            , cast (totaldue as numeric) as total
            , cast (orderdate as string) as data_pedido
            , cast (duedate as string) as data_entrega
            , cast (shipdate as string) as data_envio
            , cast (status as integer) as status
            , cast (purchaseordernumber as string) as numero_pedido_compra
            , cast (accountnumber as string) as numero_conta
            , cast (creditcardapprovalcode as string) as code_aprovacao_cartao
        from {{ source('erp', 'salesorderheader') }}
    )
select *
   -- count (id_venda) as total_pedido
from fonte_ordem
--order by id_venda