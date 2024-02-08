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
        from stg_motivo
        left join stg_ordem_motivo on
            stg_motivo.id_venda_motivo = stg_ordem_motivo.id_venda_motivo
    )
    
    /*junção anterior com pedidos*/
    , joined_pedidos as (
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

    /*junção anterior com os detalhes do pedido*/
    ,joined_detalhe as (
        select 
            joined_pedidos.id_venda
            , joined_pedidos.id_cliente
            , joined_pedidos.id_territorio
            , joined_pedidos.id_vendedor
            , joined_pedidos.id_conta_endereco
            , joined_pedidos.id_endereco
            , joined_pedidos.id_metodo_compra
            , joined_pedidos.id_cartao_credito
            , stg_detalhe_pedido.id_venda_detalhes
            , stg_detalhe_pedido.id_oferta_especial
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
            , joined_pedidos.data_entrega
            , joined_pedidos.data_envio
            , joined_pedidos.status
            , joined_pedidos.numero_pedido_compra
            , joined_pedidos.numero_conta
            , joined_pedidos.code_aprovacao_cartao
            , joined_pedidos.motivo
            , coalesce (joined_pedidos.id_venda_motivo, 0) as id_venda_motivo2
        from joined_pedidos
        left join stg_detalhe_pedido on
            joined_pedidos.id_venda = stg_detalhe_pedido.id_venda
    )

    /*criação de chave surrogate*/
    , criar_chave as (
        select
            joined_detalhe.id_venda
            , joined_detalhe.id_cliente
            , joined_detalhe.id_territorio
            , joined_detalhe.id_vendedor
            , joined_detalhe.id_conta_endereco
            , joined_detalhe.id_endereco
            , joined_detalhe.id_metodo_compra
            , joined_detalhe.id_cartao_credito
            , joined_detalhe.id_venda_detalhes
            , joined_detalhe.id_oferta_especial
            , joined_detalhe.id_produto
            , joined_detalhe.id_venda_motivo2
            , joined_detalhe.quantidade
            , joined_detalhe.preco_unidade
            , joined_detalhe.desconto_unidade
            , joined_detalhe.subtotal
            , joined_detalhe.taxa
            , joined_detalhe.frete
            , joined_detalhe.total
            , joined_detalhe.data_pedido
            , joined_detalhe.data_entrega
            , joined_detalhe.data_envio
            , joined_detalhe.status
            , joined_detalhe.numero_pedido_compra
            , joined_detalhe.numero_conta
            , joined_detalhe.code_aprovacao_cartao
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
            , criar_chave.id_metodo_compra
            , criar_chave.id_cartao_credito
            , criar_chave.id_venda_detalhes
            , criar_chave.id_produto
            , criar_chave.id_venda_motivo2
            /*datas*/
            , criar_chave.data_pedido
            , criar_chave.data_entrega
            , criar_chave.data_envio
            /*métricas*/
            , criar_chave.quantidade
            , criar_chave.preco_unidade
            , criar_chave.desconto_unidade
            , criar_chave.subtotal
            , criar_chave.taxa
            , criar_chave.frete
            , criar_chave.total
            /*categorias*/
            , criar_chave.motivo
            , criar_chave.status
            , criar_chave.numero_pedido_compra
            , criar_chave.numero_conta
            , criar_chave.code_aprovacao_cartao
        from criar_chave
    )

select *
from alteracao
order by sk_pedido