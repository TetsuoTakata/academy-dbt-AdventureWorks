with
    fonte_motivos as (
        select
            cast (salesreasonid as string) as id_venda_motivo
            , cast (name as string) as nome
            , cast (reasontype as string) as motivos 
        from {{ source('erp', 'salesreason') }}
    )
select *
from fonte_motivos