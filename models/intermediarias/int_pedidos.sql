with
    stg_pedidos as (
        select *
        from {{ ref('stg_erp__ordem') }}
    )

    ,stg_detalhe_pedido as (
        select *
        from {{ ref('stg_erp__ordem_detalhe') }}
    )

    /*junção pedidos com os detalhes do pedido*/
    , joined_detalhe as (
        select 
            /*chaves*/
            stg_pedidos.id_venda
            , stg_pedidos.id_cliente
            , stg_pedidos.id_territorio
            , stg_pedidos.id_vendedor
            , stg_pedidos.id_endereco
            , stg_pedidos.id_cartao_credito
            , stg_detalhe_pedido.id_produto
            /*data*/
            , stg_pedidos.data_pedido
            /*metrica*/
            , stg_detalhe_pedido.quantidade
            , stg_detalhe_pedido.preco_unidade
            , stg_detalhe_pedido.desconto_unidade
            , stg_pedidos.subtotal
            , stg_pedidos.taxa
            , stg_pedidos.frete
            , stg_pedidos.total
            /*categoria*/
            , stg_pedidos.status

           
            /*, ((subtotal)+(preco_unidade*quantidade*desconto_unidade))/count(joined_pedidos.id_venda)over(partition by joined_pedidos.id_venda) as teste1
            , (preco_unidade*quantidade)/count(joined_pedidos.id_venda)over(partition by joined_pedidos.id_venda) as teste2*/
        from stg_pedidos
        left join stg_detalhe_pedido on
            stg_pedidos.id_venda = stg_detalhe_pedido.id_venda
    )

    ,criar_chave2 as (
        select
            id_venda
            ,id_cliente
            ,id_territorio
            ,id_vendedor
            ,id_endereco
            ,id_cartao_credito
            ,id_produto
            ,data_pedido
            ,quantidade
            ,preco_unidade
            ,desconto_unidade
            ,subtotal
            ,taxa
            ,frete
            ,total
            ,status
        ,cast(id_venda as string) ||' - '|| cast(id_produto as string) as sk_pedido
        from joined_detalhe
    )

    ,organizacao as (
        select
            sk_pedido
            ,id_venda
            ,id_cliente
            ,id_territorio
            ,id_vendedor
            ,id_endereco
            ,id_cartao_credito
            ,id_produto
            ,data_pedido
            ,quantidade
            ,preco_unidade
            ,desconto_unidade
            ,subtotal
            ,taxa
            ,frete
            ,total
            ,status
        from criar_chave2
    )
select *
from organizacao