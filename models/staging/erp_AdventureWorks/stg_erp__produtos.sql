with
    fonte_produtos as (
        select 
            cast (productid as integer) as id_produto
            , cast (name as string) as nome_produto
            , cast (productnumber as string) as numero_produto
            , cast (safetystocklevel as integer) as produto_estoque
            , cast (reorderpoint as integer) as aviso_reabastecer
            , cast (daystomanufacture as integer) as tempo_producao
        from {{ source('erp', 'product') }}
    )
select *
from fonte_produtos