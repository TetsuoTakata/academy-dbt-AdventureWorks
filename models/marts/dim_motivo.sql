with
    stg_ordem_motivo as (
        select *
        from {{ ref('stg_erp__ordem_motivo') }}
    )

    ,stg_motivo as (
        select *
        from {{ ref('stg_erp__motivos') }}
    )

    ,joined_motivo as (
        select
            stg_ordem_motivo.id_venda
            , stg_ordem_motivo.id_venda_motivo
            , stg_motivo.motivo
        from stg_ordem_motivo
        left join stg_motivo on
            stg_ordem_motivo.id_venda_motivo = stg_motivo.id_venda_motivo
    )

    /*criação de chave surrogate*/
    , criar_chave as (
        select
            /*chaves*/
            id_venda
            ,id_venda_motivo
            /*categoria*/
            ,motivo
            ,cast(id_venda as string) ||' - '|| cast(id_venda_motivo as string) as sk_motivo
        from joined_motivo
    )

select*
from criar_chave