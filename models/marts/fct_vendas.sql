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

    , transformacoes as (
        select *
            , subtotal / count(id_venda) over(partition by id_venda) as subtotal_ponderado
            , taxa / count(id_venda) over(partition by id_venda) as taxa_ponderado
            , frete / count(id_venda) over(partition by id_venda) as frete_ponderado
            , total / count(id_venda) over(partition by id_venda) as total_ponderado
        from joined_venda
    )

    , joined_final as (
        select
            transformacoes.sk_pedido
            ,transformacoes.id_venda
            ,transformacoes.id_cliente
            ,transformacoes.id_vendedor
            ,transformacoes.id_endereco
            ,transformacoes.id_cartao_credito
            ,transformacoes.id_venda_motivo2
            ,transformacoes.id_produto
            ,transformacoes.id_provincia_estado
            ,cliente.id_loja
            ,transformacoes.quantidade
            ,transformacoes.preco_unidade
            ,transformacoes.desconto_unidade
            ,transformacoes.subtotal
            ,transformacoes.subtotal_ponderado
            ,transformacoes.taxa
            ,transformacoes.taxa_ponderado
            ,transformacoes.frete
            ,transformacoes.frete_ponderado
            ,transformacoes.total
            ,transformacoes.total_ponderado
            ,transformacoes.data_pedido
            ,transformacoes.data_entrega
            ,transformacoes.data_envio
            ,transformacoes.status
            ,transformacoes.numero_pedido_compra
            ,transformacoes.numero_conta
            ,transformacoes.code_aprovacao_cartao
            ,transformacoes.motivo
            ,transformacoes.cidade
            ,transformacoes.provincia_estado
            ,transformacoes.pais
            ,transformacoes.codigo_provincia_estado
            ,transformacoes.codigo_regiao_pais
            ,transformacoes.endereco1
            ,transformacoes.endereco2
            ,transformacoes.codigo_postal
            ,transformacoes.tipo_cartao
            ,transformacoes.numero_cartao
            ,transformacoes.expira_mes
            ,transformacoes.expira_ano           
            ,cliente.nome_loja
            ,cliente.nome_vendedor
            ,produtos.nome_produto
            ,produtos.numero_produto
            ,produtos.custo_padrao
            ,produtos.preco_tabela
            ,produtos.tamanho
            ,produtos.unidade_medida_tamanho
            ,produtos.peso
            ,produtos.unidade_medida_peso
            ,produtos.cor
            ,produtos.produto_estoque
            ,produtos.aviso_reabastecer
            ,produtos.tempo_producao
            ,produtos.inicio_venda
            ,produtos.fim_venda
        from transformacoes
        left join cliente on
            transformacoes.id_cliente = cliente.id_cliente
        left join produtos on
            transformacoes.id_produto = produtos.id_produto
    )

   /* ,vendas_em_2011 as (
        select 
            sum(subtotal_ponderado) as total_bruto
            ,sum(taxa_ponderado) as taxa_bruto
            ,sum(frete_ponderado) as frete_bruto
            --,joined_final.quantidade
            --,joined_final.preco_unidade
            --,joined_final.desconto_unidade 
        from joined_final
        where data_pedido between '2011-01-01 00:00:00.000' and '2011-12-31 00:00:00.000'
    )
select 
    total_bruto
    ,taxa_bruto
    ,frete_bruto
    , total_bruto+taxa_bruto+frete_bruto as total_bruto_final
    , quantidade*preco_unidade*(1-desconto_unidade) as total_liquido
from vendas_em_2011
where total_bruto not between 12646112 and 12646113*/

select *
    --count (distinct id_venda) as total_pedido
from joined_final
--order by sk_pedido
