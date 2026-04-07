create index if not exists idx_competitions_end_date on public.competitions (end_date);
create index if not exists idx_competitions_start_date on public.competitions (start_date);
create index if not exists idx_competitions_location_id on public.competitions (location_id);

create index if not exists idx_competition_categories_competition_id on public.competition_categories (competition_id);

create index if not exists idx_applications_athlete_id on public.applications (athlete_id);
create index if not exists idx_applications_competition_id on public.applications (competition_id);
create index if not exists idx_applications_competition_athlete on public.applications (competition_id, athlete_id);
create index if not exists idx_applications_category_id on public.applications (category_id);

create index if not exists idx_competition_bouts_competition_id on public.competition_bouts (competition_id);
create index if not exists idx_competition_bouts_category_id on public.competition_bouts (category_id);

create index if not exists idx_bouts_competition_id on public.bouts (competition_id);
create index if not exists idx_bouts_category_id on public.bouts (category_id);

create index if not exists idx_locations_type_parent on public.locations (type, parent_id);
create index if not exists idx_locations_parent_id on public.locations (parent_id);

create index if not exists idx_staff_locations_role_id on public.staff_locations (role_id);
create index if not exists idx_staff_locations_location_id on public.staff_locations (location_id);

create index if not exists idx_user_roles_user_id on public.user_roles (user_id);
