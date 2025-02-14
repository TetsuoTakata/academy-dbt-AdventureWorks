with
    stg_cliente as (
        select *
        from {{ ref('stg_erp__cliente') }}
    )

    , stg_loja as (
        select *
        from {{ ref('stg_erp__loja') }}
    )

    , stg_vendedor as (
        select *
        from {{ ref('stg_erp__vendedor') }}
    )

    /*junção de vendedor e loja à staging cliente*/
    , joined_cliente as (
        select
            stg_cliente.id_cliente
            , stg_cliente.id_vendedor
            , stg_cliente.id_loja
            , stg_loja.nome_loja
            , stg_vendedor.nome
            , stg_vendedor.nome_meio
            , stg_vendedor.sobrenome
        from stg_cliente 
        left join stg_loja on
            stg_cliente.id_loja = stg_loja.id_loja
        left join stg_vendedor on
            stg_cliente.id_vendedor = stg_vendedor.id_vendedor
    
    )

    /*unir onome, nome do meio e sobrenome*/
    , modificacao as (
        select
            /*chaves*/
            id_cliente
            , id_vendedor
            , id_loja
            /*categoria*/
            , nome_loja
            , nome
            , nome_meio
            , sobrenome
            ,case 
                when nome_meio is null
                then ' '
                else nome_meio
            end as nome_meio2
        from joined_cliente
    )

    , juncao_nome as (
        select
            /*chaves*/
            id_cliente
            , id_vendedor
            , id_loja
            /*categoria*/
            , nome_loja
            , cast(nome as string) ||' '|| cast (nome_meio2 as string) ||' '|| cast (sobrenome as string) as nome_vendedor
        from modificacao
    )

select *
from juncao_nome
