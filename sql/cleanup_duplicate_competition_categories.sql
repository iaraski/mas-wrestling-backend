begin;

create temporary table tmp_comp_cat_dups as
with ranked as (
  select
    id,
    first_value(id) over (
      partition by
        competition_id,
        gender,
        age_min,
        age_max,
        weight_min,
        weight_max,
        competition_day,
        mandate_day
      order by id asc
    ) as keep_id,
    row_number() over (
      partition by
        competition_id,
        gender,
        age_min,
        age_max,
        weight_min,
        weight_max,
        competition_day,
        mandate_day
      order by id asc
    ) as rn
  from public.competition_categories
)
select id as old_id, keep_id
from ranked
where rn > 1;

update public.applications a
set category_id = d.keep_id
from tmp_comp_cat_dups d
where a.category_id = d.old_id;

update public.competition_bouts b
set category_id = d.keep_id
from tmp_comp_cat_dups d
where b.category_id = d.old_id;

update public.bouts b
set category_id = d.keep_id
from tmp_comp_cat_dups d
where b.category_id = d.old_id;

update public.competition_category_assignments a
set category_id = d.keep_id
from tmp_comp_cat_dups d
where a.category_id = d.old_id;

delete from public.competition_categories c
using tmp_comp_cat_dups d
where c.id = d.old_id;

with ranked as (
  select
    id,
    row_number() over (partition by competition_id, category_id order by updated_at desc nulls last, id asc) as rn
  from public.competition_category_assignments
)
delete from public.competition_category_assignments a
using ranked r
where a.id = r.id
  and r.rn > 1;

create unique index if not exists competition_categories_unique_idx
on public.competition_categories (
  competition_id,
  gender,
  age_min,
  age_max,
  weight_min,
  weight_max,
  competition_day,
  mandate_day
);

commit;
