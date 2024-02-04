with
    stg_produtos as (
        select 
            id_produto
            , nome_produto
            , numero_produto
            , custo_padrao
            , preco_tabela
            , tamanho
            , unidade_medida_tamanho
            , peso
            , unidade_medida_peso
            , cor
            , produto_estoque
            , aviso_reabastecer
            , tempo_producao
            , inicio_venda
            , fim_venda
        from {{ ref('stg_erp__produtos') }}
    )
select *
from stg_produtos