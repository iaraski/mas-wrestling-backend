create extension if not exists pgcrypto;

create table if not exists public.auth_passwords (
  user_id uuid primary key references public.users(id) on delete cascade,
  password_salt text not null,
  password_hash text not null,
  iterations integer not null,
  updated_at timestamptz not null default now()
);

alter table public.auth_passwords enable row level security;

drop policy if exists "service role read auth_passwords" on public.auth_passwords;
drop policy if exists "service role write auth_passwords" on public.auth_passwords;

create policy "service role read auth_passwords"
on public.auth_passwords
for select
to service_role
using (true);

create policy "service role write auth_passwords"
on public.auth_passwords
for all
to service_role
using (true)
with check (true);
