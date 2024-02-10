with
    fonte_cartao_credito as (
        select
            cast (creditcardid as integer) id_cartao_credito
            , cast (cardtype as string) as tipo_cartao
        from {{ source('erp', 'creditcard') }}
    )
select *
from fonte_cartao_credito