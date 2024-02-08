with
    stg_produtos as (
        select 
            /*chave*/
            id_produto
            /*categorias*/
            , nome_produto
            , numero_produto
            /*metricas*/
            , tamanho
            , unidade_medida_tamanho
            , peso
            , unidade_medida_peso
            /*netricas*/
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