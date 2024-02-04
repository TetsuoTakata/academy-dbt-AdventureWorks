with
    stg_pedidos as(
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

    ,joined_motivo as (
        select
            stg_ordem_motivo.id_venda
            , stg_ordem_motivo.id_venda_motivo
            , stg_motivo.motivo
        from stg_motivo
        left join stg_ordem_motivo on
            stg_motivo.id_venda_motivo = stg_ordem_motivo.id_venda_motivo
    )
    
    , joined_pedido as (
        select
            stg_pedidos.id_venda
            , stg_pedidos.id_cliente
            , stg_pedidos.id_territorio
            , stg_pedidos.id_vendedor
            , stg_pedidos.id_conta_endereco
            , stg_pedidos.id_endereco
            , stg_pedidos.id_metodo_compra
            , stg_pedidos.id_cartao_credito
            , stg_pedidos.subtotal
            , stg_pedidos.taxa
            , stg_pedidos.frete
            , stg_pedidos.total
            , stg_pedidos.data_pedido
            , stg_pedidos.data_entrega
            , stg_pedidos.data_envio
            , stg_pedidos.status
            , stg_pedidos.numero_pedido_compra
            , stg_pedidos.numero_conta
            , stg_pedidos.code_aprovacao_cartao
            , joined_motivo.id_venda_motivo
            , joined_motivo.motivo
        from stg_pedidos
        left join joined_motivo on
            stg_pedidos.id_venda = joined_motivo.id_venda
    )

select*
from joined_pedido