with
    cliente as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , endereco as (
        select *
        from {{ ref('dim_endereco') }}
    )

    , pedido as (
        select *
        from {{ ref('int_pedidos') }}
    )

    , cartao_credito as (
        select *
        from {{ ref('stg_erp__cartao_credito') }}
    )

    /*junção dos pedidos*/
    , joined_venda as (
        select 
            pedido.sk_pedido
            , pedido.id_venda
            , pedido.id_cliente
            , pedido.id_vendedor
            , pedido.id_endereco
            , pedido.id_cartao_credito
            , pedido.id_venda_motivo2
            , pedido.id_produto
            , endereco.id_provincia_estado
            , quantidade
            , preco_unidade
            , desconto_unidade
            , pedido.subtotal
            , pedido.taxa
            , pedido.frete
            , pedido.total
            , pedido.data_pedido
            , pedido.status
            , pedido.motivo
            , endereco.cidade
            , endereco.provincia_estado
            , endereco.pais
            , endereco.codigo_provincia_estado
            , endereco.codigo_regiao_pais
            , endereco.endereco1
            , endereco.endereco2
            , cartao_credito.tipo_cartao
        from pedido
        left join endereco on
            pedido.id_endereco = endereco.id_endereco
        left join cartao_credito on
            pedido.id_cartao_credito = cartao_credito.id_cartao_credito
    )

    /*transformações das informaçãos*/
    , transformacoes as (
        select *
            , quantidade*preco_unidade as total_bruto
            , quantidade*preco_unidade*(1-desconto_unidade) as total_liquido
            , subtotal / count(id_venda) over(partition by id_venda) as subtotal_ponderado
        from joined_venda
    )

    /*transformações dos dados*/
    , transformacoes2 as (
        select *
            , total_liquido / count(id_venda) over(partition by id_venda) as total_liquido_ponderado
            , total_bruto / count(id_venda) over(partition by id_venda) as total_bruto_ponderado
        from transformacoes
    )

    , joined_final as (
        select
            /*chave surrogate*/
            transformacoes2.sk_pedido
            /*chave secundária*/
            ,transformacoes2.id_venda
            ,transformacoes2.id_cliente
            ,transformacoes2.id_vendedor
            ,transformacoes2.id_cartao_credito
            ,transformacoes2.id_venda_motivo2
            ,transformacoes2.id_produto
            ,cliente.id_loja
            /*data*/
            ,transformacoes2.data_pedido
            /*metrica*/
            ,transformacoes2.quantidade
            ,transformacoes2.preco_unidade
            ,transformacoes2.desconto_unidade
            ,transformacoes2.total_bruto
            ,transformacoes2.total_bruto_ponderado
            ,transformacoes2.total_liquido
            ,transformacoes2.total_liquido_ponderado
            ,transformacoes2.subtotal
            ,transformacoes2.subtotal_ponderado
            --,transformacoes2.taxa
            --,transformacoes2.frete
            --,transformacoes2.total
            ,produtos.tempo_producao
            /*categorias*/
            ,transformacoes2.status
            ,transformacoes2.motivo
            ,transformacoes2.cidade
            ,transformacoes2.provincia_estado
            ,transformacoes2.pais
            ,transformacoes2.codigo_provincia_estado
            ,transformacoes2.codigo_regiao_pais
            ,transformacoes2.endereco1
            ,transformacoes2.endereco2
            ,transformacoes2.tipo_cartao           
            ,cliente.nome_loja
            ,cliente.nome_vendedor
            ,produtos.nome_produto
            ,produtos.numero_produto
            ,produtos.produto_estoque
            ,produtos.aviso_reabastecer
        from transformacoes2
        left join cliente on
            transformacoes2.id_cliente = cliente.id_cliente
        left join produtos on
            transformacoes2.id_produto = produtos.id_produto
    )

select *
from joined_final
