with
    fonte_ordem_detalhes as (
        select
            cast (salesorderid as integer) as id_venda
            ,cast (productid as integer) as id_produto
            ,cast (orderqty as integer) as quantidade
            ,cast (unitprice as numeric) as preco_unidade
            ,cast (unitpricediscount as numeric) as desconto_unidade
        from {{ source('erp', 'salesorderdetail') }}
    )
select*
from fonte_ordem_detalhes