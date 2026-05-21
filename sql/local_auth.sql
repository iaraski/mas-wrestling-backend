create extension if not exists pgcrypto;

create table if not exists public.auth_passwords (
  user_id uuid primary key references public.users(id) on delete cascade,
  password_salt text not null,
  password_hash text not null,
  iterations integer not null,
  updated_at timestamptz not null default now()
);

alter table public.auth_passwords disable row level security;
