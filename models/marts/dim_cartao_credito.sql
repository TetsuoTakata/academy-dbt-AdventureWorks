with
    stg_cartao_credito as (
        select
            id_cartao_credito
            ,tipo_cartao
        from {{ ref('stg_erp__cartao_credito') }}
    )
select *
from stg_cartao_credito