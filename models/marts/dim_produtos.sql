with
    stg_produtos as (
        select 
            /*chave*/
            id_produto
            /*categorias*/
            , nome_produto
            , numero_produto
            /*netricas*/
            , produto_estoque
            , aviso_reabastecer
            , tempo_producao
        from {{ ref('stg_erp__produtos') }}
    )
select *
from stg_produtos