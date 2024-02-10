with
    stg_pedidos as (
        select *
        from {{ ref('stg_erp__ordem') }}
    )

    ,stg_ordem_motivo as (
        select *
        from {{ ref('stg_erp__ordem_motivo') }}
    )

    ,stg_motivo as (
        select *
        from {{ ref('stg_erp__motivos') }}
    )

    ,stg_detalhe_pedido as (
        select *
        from {{ ref('stg_erp__ordem_detalhe') }}
    )

    /*junção de motivo com ordem_motivo*/
    ,joined_motivo as (
        select
            stg_ordem_motivo.id_venda
            , stg_ordem_motivo.id_venda_motivo
            , stg_motivo.motivo
        from stg_ordem_motivo
        left join stg_motivo on
            stg_ordem_motivo.id_venda_motivo = stg_motivo.id_venda_motivo
    )
    
    /*junção anterior com pedidos*/
    , joined_pedidos as (
        select
            stg_pedidos.id_venda
            , stg_pedidos.id_cliente
            , stg_pedidos.id_territorio
            , stg_pedidos.id_vendedor
            , stg_pedidos.id_endereco
            , stg_pedidos.id_cartao_credito
            , stg_pedidos.subtotal
            , stg_pedidos.taxa
            , stg_pedidos.frete
            , stg_pedidos.total
            , stg_pedidos.data_pedido
            , stg_pedidos.status
            , joined_motivo.id_venda_motivo
            , joined_motivo.motivo
        from stg_pedidos
        left join joined_motivo on
            stg_pedidos.id_venda = joined_motivo.id_venda
    )

    /*junção anterior com os detalhes do pedido*/
    , joined_detalhe as (
        select 
            joined_pedidos.id_venda
            , joined_pedidos.id_cliente
            , joined_pedidos.id_territorio
            , joined_pedidos.id_vendedor
            , joined_pedidos.id_endereco
            , joined_pedidos.id_cartao_credito
            , stg_detalhe_pedido.id_produto
            , joined_pedidos.id_venda_motivo
            , stg_detalhe_pedido.quantidade
            , stg_detalhe_pedido.preco_unidade
            , stg_detalhe_pedido.desconto_unidade
            , joined_pedidos.subtotal
            , joined_pedidos.taxa
            , joined_pedidos.frete
            , joined_pedidos.total
            , joined_pedidos.data_pedido
            , joined_pedidos.status
            , joined_pedidos.motivo
            , coalesce (joined_pedidos.id_venda_motivo, 0) as id_venda_motivo2
            , ((subtotal)+(preco_unidade*quantidade*desconto_unidade))/count(joined_pedidos.id_venda)over(partition by joined_pedidos.id_venda) as teste1
            , (preco_unidade*quantidade)/count(joined_pedidos.id_venda)over(partition by joined_pedidos.id_venda) as teste2
        from stg_detalhe_pedido
        left join joined_pedidos on
            stg_detalhe_pedido.id_venda = joined_pedidos.id_venda
    )

    /*criação de chave surrogate*/
    , criar_chave as (
        select
            joined_detalhe.id_venda
            , joined_detalhe.id_cliente
            , joined_detalhe.id_territorio
            , joined_detalhe.id_vendedor
            , joined_detalhe.id_endereco
            , joined_detalhe.id_cartao_credito
            , joined_detalhe.id_produto
            , joined_detalhe.id_venda_motivo2
            , joined_detalhe.quantidade
            , joined_detalhe.preco_unidade
            , joined_detalhe.desconto_unidade
            ,joined_detalhe.teste1
            , joined_detalhe.subtotal
            , joined_detalhe.taxa
            , joined_detalhe.frete
            , joined_detalhe.total
            , joined_detalhe.data_pedido
            , joined_detalhe.status
            , joined_detalhe.motivo
            , cast(id_venda as string) ||' - '|| cast(id_produto as string) ||' - '|| cast(id_venda_motivo2 as string) as sk_pedido
        from joined_detalhe
    )

    , alteracao as (
        select
            /*chave surrogate*/
            sk_pedido
            /*chaves secundárias*/
            , criar_chave.id_venda
            , criar_chave.id_cliente
            , criar_chave.id_territorio
            , criar_chave.id_vendedor
            , criar_chave.id_endereco
            , criar_chave.id_cartao_credito
            , criar_chave.id_produto
            , criar_chave.id_venda_motivo2
            /*datas*/
            , criar_chave.data_pedido
            /*métricas*/
            , criar_chave.quantidade
            , criar_chave.preco_unidade
            , criar_chave.desconto_unidade
            ,criar_chave.teste1
            , criar_chave.subtotal
            , criar_chave.taxa
            , criar_chave.frete
            , criar_chave.total
            /*categorias*/
            , criar_chave.motivo
            , criar_chave.status
        from criar_chave
    )

select *
from alteracao