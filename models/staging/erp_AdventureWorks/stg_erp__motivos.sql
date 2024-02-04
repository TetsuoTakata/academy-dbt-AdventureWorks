with
    fonte_motivos as (
        select
            cast (salesreasonid as integer) as id_venda_motivo
            , cast (name as string) as motivo
        from {{ source('erp', 'salesreason') }}
    )
select *
from fonte_motivos