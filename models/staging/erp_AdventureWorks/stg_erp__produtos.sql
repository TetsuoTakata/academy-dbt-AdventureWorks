with
    fonte_produtos as (
        select 
            cast (productid as integer) as id_produto
            , cast (name as string) as nome_produto
            , cast (productnumber as string) as numero_produto
            , cast (makeflag as boolean) as bandeira_fazer
            , cast (finishedgoodsflag as boolean) as bandeira_finalizada
            , cast (color as string) as cor
            , cast (safetystocklevel as integer) as produto_estoque
            , cast (reorderpoint as integer) as aviso_reabastecer
            , cast (standardcost as numeric) as custo_padrao
            , cast (listprice as numeric) as preco_tabela
            , cast (size as string) as tamanho
            , cast (sizeunitmeasurecode as string) as unidade_medida_tamanho
            , cast (weightunitmeasurecode as string) as unidade_medida_peso
            , cast (weight as numeric) as peso
            , cast (daystomanufacture as integer) as tempo_producao
            , cast (productline as string) as linha_produto
            , cast (class as string) as classe
            , cast (style as string) as estilo
            , cast (productsubcategoryid as integer) as id_subcategoria_produto
            , cast (productmodelid as integer) as id_modelo_produto
            , cast (sellstartdate as string) as inicio_venda
            , cast (sellenddate as string) as fim_venda
        from {{ source('erp', 'product') }}
    )
select *
from fonte_produtos