with
    fonte_cartao_credito as (
        select
            cast (creditcardid as integer) id_cartao_credito
            , cast (cardtype as string) as tipo_cartao
            , cast (cardnumber as integer) as numero_cartao
            , cast (expmonth as integer) as expira_mes
            , cast (expyear as integer) as expira_ano
        from {{ source('erp', 'creditcard') }}
    )
select *
from fonte_cartao_credito