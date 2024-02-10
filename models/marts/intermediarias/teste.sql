with
    stg_pedidos2 as(
        select *
        from {{ ref('stg_erp__ordem') }}
    )

    , stg_detalhe_pedido as (
        select *
        from {{ ref('stg_erp__ordem_detalhe') }}
    )

    , joined as (
        select 
            stg_pedidos2.id_venda_pedido
            , stg_pedidos2.id_cliente
            , stg_pedidos2.id_endereco
            , stg_pedidos2.subtotal
            , stg_pedidos2.total
            , stg_pedidos2.data_pedido
            , stg_pedidos2.status

            , stg_detalhe_pedido.id_venda_detalhe
            , stg_detalhe_pedido.id_produto
            , stg_detalhe_pedido.quantidade
            , stg_detalhe_pedido.preco_unidade
            , stg_detalhe_pedido.desconto_unidade
        from stg_detalhe_pedido
        left join stg_pedidos2 on
        stg_detalhe_pedido.id_venda_detalhe = stg_pedidos2.id_venda_pedido
    )

    , soma as (
        select
        *
        , ((subtotal)+(preco_unidade*quantidade*desconto_unidade))/count(id_venda_detalhe)over(partition by id_venda_detalhe) as soma_coluna
        from joined
    )

select sum(soma_coluna)
from soma