with
    fonte_ordem_motivo as (
        select
            cast(salesorderid as integer) as id_venda
            , cast(salesreasonid as integer) as id_venda_motivo
        from {{ source('erp', 'salesorderheadersalesreason') }}
    )
select*
from fonte_ordem_motivo