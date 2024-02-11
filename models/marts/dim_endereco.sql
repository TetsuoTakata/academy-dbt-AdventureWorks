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

    /*junção de endereço, província/estado e país*/
    , joined_endereco as (
        select
            /*chaves*/
            stg_endereco.id_endereco
            , stg_provincia_estado.id_provincia_estado
            , stg_provincia_estado.id_territorio
            /*categorias*/
            , stg_endereco.endereco1
            , stg_endereco.endereco2
            , stg_endereco.cidade
            , stg_provincia_estado.provincia_estado 
            , stg_pais.pais           
            , stg_provincia_estado.codigo_provincia_estado
            , stg_provincia_estado.codigo_regiao_pais
        from  stg_provincia_estado
        left join stg_endereco on
            stg_provincia_estado.id_provincia_estado = stg_endereco.id_provincia_estado
        left join stg_pais on
            stg_provincia_estado.codigo_regiao_pais = stg_pais.codigo_regiao_pais
    )
select *
from joined_endereco

