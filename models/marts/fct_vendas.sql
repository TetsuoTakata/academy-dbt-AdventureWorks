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

    ,motivo as (
        select *
        from {{ ref('dim_motivo') }}
    )

    , cartao_credito as (
        select *
        from {{ ref('dim_cartao_credito') }}
    )

    /*junção dos pedidos*/
    , joined_venda as (
        select 
            /*chaves*/
            pedido.sk_pedido
            ,pedido.id_venda
            ,motivo.id_venda_motivo
            --,pedido.id_cliente
            --,pedido.id_territorio
            --,pedido.id_vendedor
            --,pedido.id_endereco
            --,pedido.id_cartao_credito
            ,pedido.id_produto
            --,cliente.id_vendedor
            --,cliente.id_loja
            --,endereco.id_provincia_estado
            --,endereco.id_territorio
            /*data*/
            ,pedido.data_pedido
            /*metricas*/
            ,pedido.quantidade
            ,pedido.preco_unidade
            ,pedido.desconto_unidade
            ,pedido.subtotal
            ,pedido.taxa
            ,pedido.frete
            ,pedido.total
            ,pedido.status
            /*categorias*/
            ,cliente.nome_loja
            ,cliente.nome_vendedor
            ,produtos.nome_produto
            ,produtos.numero_produto
            ,produtos.produto_estoque
            ,produtos.aviso_reabastecer
            ,produtos.tempo_producao
            ,endereco.endereco1
            ,endereco.endereco2
            ,endereco.cidade
            ,endereco.provincia_estado
            ,endereco.pais
            ,endereco.codigo_provincia_estado
            ,endereco.codigo_regiao_pais
            ,motivo.motivo
            ,cartao_credito.tipo_cartao
        from pedido
        left join cliente on
            pedido.id_cliente = cliente.id_cliente
        left join produtos on
            pedido.id_produto = produtos.id_produto
        left join endereco on
            pedido.id_endereco = endereco.id_endereco
        left join motivo on
            pedido.id_venda = motivo.id_venda
        left join cartao_credito on
            pedido.id_cartao_credito = cartao_credito.id_cartao_credito
    )

    /*transformações das informaçãos*/
    , transformacoes as (
        select *
            , coalesce (joined_venda.id_venda_motivo, 0) as id_venda_motivo2
            , quantidade*preco_unidade as total_bruto
            , quantidade*preco_unidade*(1-desconto_unidade) as total_liquido
            , subtotal / count(id_venda) over(partition by id_venda) as subtotal_ponderado
            , (quantidade*preco_unidade*(1-desconto_unidade)) / count(id_venda) over(partition by id_venda) as total_liquido_ponderado
            , (quantidade*preco_unidade) / count(sk_pedido) over(partition by sk_pedido) as total_bruto_ponderado
        from joined_venda
    )

    ,final as (
        select 
            cast(sk_pedido as string) ||' - '|| cast(id_venda_motivo2 as string) as sk_final
            ,sk_pedido
            ,id_venda
            ,id_venda_motivo
            ,id_venda_motivo2
            ,id_produto
            ,data_pedido
            ,quantidade
            ,preco_unidade
            ,desconto_unidade
            ,subtotal
            ,taxa
            ,frete
            ,total
            ,total_bruto
            ,total_liquido
            ,subtotal_ponderado
            ,total_liquido_ponderado
            ,total_bruto_ponderado
            ,status
            ,nome_loja
            ,nome_vendedor
            ,nome_produto
            ,numero_produto
            ,produto_estoque
            ,aviso_reabastecer
            ,tempo_producao
            ,endereco1
            ,endereco2
            ,cidade
            ,provincia_estado
            ,pais
            ,codigo_provincia_estado
            ,codigo_regiao_pais
            ,motivo
            ,tipo_cartao
        from transformacoes
    )

select *
from final
