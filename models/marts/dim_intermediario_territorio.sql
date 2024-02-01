with
    joined_territorio as (
        select *
        from stg_erp__provincia_estado
        left join stg_erp__territorio on
            stg_erp__provincia_estado.id_territorio = stg_erp__territorio.id_territorio
    )
select *
from joined_territorio