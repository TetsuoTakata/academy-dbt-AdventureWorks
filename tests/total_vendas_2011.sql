/* verificação se as vendas brutas no ano de 2011 foram de $ 12646112,16 */

with
    vendas_em_2011 as (
        select sum(total_bruto_ponderado) as total_bruto
        from {{ ref('fct_vendas') }}
        where data_pedido between '2011-01-01' and '2011-12-31'
    )
select total_bruto
from vendas_em_2011
where total_bruto not between 12646112 and 12646113