with
    stg_provincia_estado as (
        select *
        from {{ ref('stg_erp__provincia_estado') }}
    )

    , stg_endereco as (
        select *
        from {{ ref('stg_erp__endereco') }}
    )

    , stg_pais as (
        select *
        from {{ ref('stg_erp__pais') }}
    )

    , joined_endereco as (
        select
            stg_endereco.id_endereco
            ,stg_provincia_estado.id_provincia_estado
            , stg_provincia_estado.id_territorio
            , stg_endereco.cidade
            , stg_provincia_estado.provincia_estado         
            , stg_provincia_estado.codigo_provincia_estado
            , stg_provincia_estado.codigo_regiao_pais
            , stg_endereco.endereco1
            , stg_endereco.endereco2
            , stg_endereco.codigo_postal
        from stg_endereco
        left join stg_provincia_estado on
            stg_endereco.id_provincia_estado = stg_provincia_estado.id_provincia_estado
    )

    , joined_endereco2 as (
        select
            joined_endereco.id_endereco
            , joined_endereco.id_provincia_estado
            , joined_endereco.id_territorio
            , joined_endereco.cidade
            , joined_endereco.provincia_estado 
            , stg_pais.pais           
            , joined_endereco.codigo_provincia_estado
            , joined_endereco.codigo_regiao_pais
            , joined_endereco.endereco1
            , joined_endereco.endereco2
            , joined_endereco.codigo_postal
        from joined_endereco
        left join stg_pais on
            joined_endereco.codigo_regiao_pais = stg_pais.codigo_regiao_pais

    )
select *
from joined_endereco2

