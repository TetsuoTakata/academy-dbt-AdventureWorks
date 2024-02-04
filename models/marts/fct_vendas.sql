with
    endereco as (
        select *
        from {{ ref('int_endereco') }}
    )

    , pedido as (
        select *
        from {{ ref('int_pedidos') }}
    )

    , cartao_credito as (
        select *
        from {{ ref('stg_erp__cartao_credito') }}
    )

    , joined_venda as (
        select 
            pedido.id_venda
            , pedido.id_cliente
            , pedido.id_territorio
            , pedido.id_vendedor
            , pedido.id_conta_endereco
            , pedido.id_endereco
            , pedido.id_metodo_compra
            , pedido.id_cartao_credito
            , pedido.id_venda_motivo
            , endereco.id_provincia_estado
            , pedido.subtotal
            , pedido.taxa
            , pedido.frete
            , pedido.total
            , pedido.data_pedido
            , pedido.data_entrega
            , pedido.data_envio
            , pedido.status
            , pedido.numero_pedido_compra
            , pedido.numero_conta
            , pedido.code_aprovacao_cartao
            , pedido.motivo
            , endereco.cidade
            , endereco.provincia_estado
            , endereco.pais
            , endereco.codigo_provincia_estado
            , endereco.codigo_regiao_pais
            , endereco.endereco1
            , endereco.endereco2
            , endereco.codigo_postal
            , cartao_credito.tipo_cartao
            , cartao_credito.numero_cartao
            , cartao_credito.expira_mes
            , cartao_credito.expira_ano
        from pedido
        left join endereco on
            pedido.id_endereco = endereco.id_endereco
        left join cartao_credito on
            pedido.id_cartao_credito = cartao_credito.id_cartao_credito
    )
select *
from joined_venda